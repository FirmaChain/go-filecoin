package chain

import (
	"context"
	"sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"

	"github.com/filecoin-project/go-filecoin/clock"
	"github.com/filecoin-project/go-filecoin/consensus"
	"github.com/filecoin-project/go-filecoin/metrics"
	"github.com/filecoin-project/go-filecoin/metrics/tracing"
	"github.com/filecoin-project/go-filecoin/net"
	"github.com/filecoin-project/go-filecoin/types"
)

var reorgCnt *metrics.Int64Counter

func init() {
	reorgCnt = metrics.NewInt64Counter("chain/reorg_count", "The number of reorgs that have occurred.")
}

// UntrustedChainHeightLimit is the maximum number of blocks ahead of the current consensus
// chain height to accept if syncing without trust.
var UntrustedChainHeightLimit = 600
var (
	// ErrChainHasBadTipSet is returned when the syncer traverses a chain with a cached bad tipset.
	ErrChainHasBadTipSet = errors.New("input chain contains a cached bad tipset")
	// ErrNewChainTooLong is returned when processing a fork that split off from the main chain too many blocks ago.
	ErrNewChainTooLong = errors.New("input chain forked from best chain too far in the past")
	// ErrUnexpectedStoreState indicates that the syncer's chain store is violating expected invariants.
	ErrUnexpectedStoreState = errors.New("the chain store is in an unexpected state")
)

var syncOneTimer *metrics.Float64Timer

func init() {
	syncOneTimer = metrics.NewTimerMs("syncer/sync_one", "Duration of single tipset validation in milliseconds")
}

var logSyncer = logging.Logger("chain.syncer")

type syncerChainReaderWriter interface {
	GetHead() types.TipSetKey
	GetTipSet(tsKey types.TipSetKey) (types.TipSet, error)
	GetTipSetStateRoot(tsKey types.TipSetKey) (cid.Cid, error)
	HasTipSetAndState(ctx context.Context, tsKey types.TipSetKey) bool
	PutTipSetAndState(ctx context.Context, tsas *TipSetAndState) error
	SetHead(ctx context.Context, s types.TipSet) error
	HasTipSetAndStatesWithParentsAndHeight(pTsKey types.TipSetKey, h uint64) bool
	GetTipSetAndStatesByParentsAndHeight(pTsKey types.TipSetKey, h uint64) ([]*TipSetAndState, error)
}

type syncChainSelector interface {
	// IsHeaver returns true if tipset a is heavier than tipset b and false if
	// tipset b is heavier than tipset a.
	IsHeavier(ctx context.Context, a, b types.TipSet, aStateID, bStateID cid.Cid) (bool, error)
	// NewWeight returns the weight of a tipset after the upgrade to version 1
	NewWeight(ctx context.Context, ts types.TipSet, stRoot cid.Cid) (uint64, error)
}

type syncStateEvaluator interface {
	// RunStateTransition returns the state root CID resulting from applying the input ts to the
	// prior `stateRoot`.  It returns an error if the transition is invalid.
	RunStateTransition(ctx context.Context, ts types.TipSet, tsMessages [][]*types.SignedMessage, tsReceipts [][]*types.MessageReceipt, ancestors []types.TipSet, parentWeight uint64, stateID cid.Cid) (cid.Cid, error)
}

// Syncer updates its chain.Store according to the methods of its
// consensus.Protocol.  It uses a bad tipset cache and a limit on new
// blocks to traverse during chain collection.  The Syncer can query the
// network for blocks.  The Syncer maintains the following invariant on
// its store: all tipsets that pass the syncer's validity checks are added to the
// chain store along with their state root CID.
//
// Ideally the code that syncs the chain according to consensus rules should
// be independent of any particular implementation of consensus.  Currently the
// Syncer is coupled to details of Expected Consensus. This dependence
// exists in the widen function, the fact that widen is called on only one
// tipset in the incoming chain, and assumptions regarding the existence of
// grandparent state in the store.
type Syncer struct {
	// This mutex ensures at most one call to HandleNewTipSet executes at
	// any time.  This is important because at least two sections of the
	// code otherwise have races:
	// 1. syncOne assumes that chainStore.Head() does not change when
	// comparing tipset weights and updating the store
	// 2. HandleNewTipSet assumes that calls to widen and then syncOne
	// are not run concurrently with other calls to widen to ensure
	// that the syncer always finds the heaviest existing tipset.
	mu sync.Mutex
	// fetcher is the networked block fetching service for fetching blocks
	// and messages.
	fetcher net.Fetcher
	// badTipSetCache is used to filter out collections of invalid blocks.
	badTipSets *badTipSetCache

	// Evaluates tipset messages and stores the resulting states.
	stateEvaluator syncStateEvaluator
	// Selects the heaviest of two chains
	chainSelector syncChainSelector
	// Provides and stores validated tipsets and their state roots.
	chainStore syncerChainReaderWriter
	// Provides message collections given cids
	messageProvider MessageProvider

	clock clock.Clock

	// Reporter is used by the syncer to update the current status of the chain.
	reporter Reporter
}

// NewSyncer constructs a Syncer ready for use.
func NewSyncer(e syncStateEvaluator, cs syncChainSelector, s syncerChainReaderWriter, m MessageProvider, f net.Fetcher, sr Reporter, c clock.Clock) *Syncer {
	return &Syncer{
		fetcher: f,
		badTipSets: &badTipSetCache{
			bad: make(map[string]struct{}),
		},
		stateEvaluator:  e,
		chainSelector:   cs,
		chainStore:      s,
		messageProvider: m,
		clock:           c,
		reporter:        sr,
	}
}

// syncOne syncs a single tipset with the chain store. syncOne calculates the
// parent state of the tipset and calls into consensus to run a state transition
// in order to validate the tipset.  In the case the input tipset is valid,
// syncOne calls into consensus to check its weight, and then updates the head
// of the store if this tipset is the heaviest.
//
// Precondition: the caller of syncOne must hold the syncer's lock (syncer.mu) to
// ensure head is not modified by another goroutine during run.
func (syncer *Syncer) syncOne(ctx context.Context, grandParent, parent, next types.TipSet) error {
	priorHeadKey := syncer.chainStore.GetHead()

	// if tipset is already priorHeadKey, we've been here before. do nothing.
	if priorHeadKey.Equals(next.Key()) {
		return nil
	}

	stopwatch := syncOneTimer.Start(ctx)
	defer stopwatch.Stop(ctx)

	// Lookup parent state root. It is guaranteed by the syncer that it is in the chainStore.
	stateRoot, err := syncer.chainStore.GetTipSetStateRoot(parent.Key())
	if err != nil {
		return err
	}

	// Gather ancestor chain needed to process state transition.
	h, err := next.Height()
	if err != nil {
		return err
	}
	ancestorHeight := types.NewBlockHeight(h).Sub(types.NewBlockHeight(consensus.AncestorRoundsNeeded))
	ancestors, err := GetRecentAncestors(ctx, parent, syncer.chainStore, ancestorHeight)
	if err != nil {
		return err
	}

	// Gather tipset messages
	var nextMessages [][]*types.SignedMessage
	var nextReceipts [][]*types.MessageReceipt
	for i := 0; i < next.Len(); i++ {
		blk := next.At(i)
		msgs, err := syncer.messageProvider.LoadMessages(ctx, blk.Messages)
		if err != nil {
			return errors.Wrapf(err, "syncing tip %s failed loading message list %s for block %s", next.Key(), blk.Messages, blk.Cid())
		}
		rcpts, err := syncer.messageProvider.LoadReceipts(ctx, blk.MessageReceipts)
		if err != nil {
			return errors.Wrapf(err, "syncing tip %s failed loading receipts list %s for block %s", next.Key(), blk.MessageReceipts, blk.Cid())
		}
		nextMessages = append(nextMessages, msgs)
		nextReceipts = append(nextReceipts, rcpts)
	}

	// Gather validated parent weight
	parentWeight, err := syncer.calculateParentWeight(ctx, parent, grandParent)
	if err != nil {
		return err
	}

	// Run a state transition to validate the tipset and compute
	// a new state to add to the store.
	root, err := syncer.stateEvaluator.RunStateTransition(ctx, next, nextMessages, nextReceipts, ancestors, parentWeight, stateRoot)
	if err != nil {
		return err
	}
	err = syncer.chainStore.PutTipSetAndState(ctx, &TipSetAndState{
		TipSet:          next,
		TipSetStateRoot: root,
	})
	if err != nil {
		return err
	}
	logSyncer.Debugf("Successfully updated store with %s", next.String())

	// TipSet is validated and added to store, now check if it is the heaviest.
	nextParentStateID, err := syncer.chainStore.GetTipSetStateRoot(parent.Key())
	if err != nil {
		return err
	}

	headTipSet, err := syncer.chainStore.GetTipSet(priorHeadKey)
	if err != nil {
		return err
	}
	headParentKey, err := headTipSet.Parents()
	if err != nil {
		return err
	}

	var headParentStateID cid.Cid
	if !headParentKey.Empty() { // head is not genesis
		headParentStateID, err = syncer.chainStore.GetTipSetStateRoot(headParentKey)
		if err != nil {
			return err
		}
	}

	heavier, err := syncer.chainSelector.IsHeavier(ctx, next, headTipSet, nextParentStateID, headParentStateID)
	if err != nil {
		return err
	}

	// If it is the heaviest update the chainStore.
	if heavier {
		if err = syncer.chainStore.SetHead(ctx, next); err != nil {
			return err
		}
		// Gather the entire new chain for reorg comparison and logging.
		syncer.logReorg(ctx, headTipSet, next)
	}

	return nil
}

// TODO #3537 this should be stored the first time it is computed and retrieved
// from disk just like aggregate state roots.
func (syncer *Syncer) calculateParentWeight(ctx context.Context, parent, grandParent types.TipSet) (uint64, error) {
	if grandParent.Equals(types.UndefTipSet) {
		return syncer.chainSelector.NewWeight(ctx, parent, cid.Undef)
	}
	gpStRoot, err := syncer.chainStore.GetTipSetStateRoot(grandParent.Key())
	if err != nil {
		return 0, err
	}
	return syncer.chainSelector.NewWeight(ctx, parent, gpStRoot)
}

// ancestorsFromStore returns the parent and grandparent tipsets of `ts`
func (syncer *Syncer) ancestorsFromStore(ts types.TipSet) (types.TipSet, types.TipSet, error) {
	parentCids, err := ts.Parents()
	if err != nil {
		return types.UndefTipSet, types.UndefTipSet, err
	}
	parent, err := syncer.chainStore.GetTipSet(parentCids)
	if err != nil {
		return types.UndefTipSet, types.UndefTipSet, err
	}
	grandParentCids, err := parent.Parents()
	if err != nil {
		return types.UndefTipSet, types.UndefTipSet, err
	}
	if grandParentCids.Empty() {
		// parent == genesis ==> grandParent undef
		return parent, types.UndefTipSet, nil
	}
	grandParent, err := syncer.chainStore.GetTipSet(grandParentCids)
	if err != nil {
		return types.UndefTipSet, types.UndefTipSet, err
	}
	return parent, grandParent, nil
}

func (syncer *Syncer) logReorg(ctx context.Context, curHead, newHead types.TipSet) {
	curHeadIter := IterAncestors(ctx, syncer.chainStore, curHead)
	newHeadIter := IterAncestors(ctx, syncer.chainStore, newHead)
	commonAncestor, err := FindCommonAncestor(curHeadIter, newHeadIter)
	if err != nil {
		// Should never get here because reorgs should always have a
		// common ancestor..
		logSyncer.Warningf("unexpected error when running FindCommonAncestor for reorg log: %s", err.Error())
		return
	}

	reorg := IsReorg(curHead, newHead, commonAncestor)
	if reorg {
		reorgCnt.Inc(ctx, 1)
		dropped, added, err := ReorgDiff(curHead, newHead, commonAncestor)
		if err == nil {
			logSyncer.Infof("reorg dropping %d height and adding %d height from %s to %s", dropped, added, curHead.String(), newHead.String())
		} else {
			logSyncer.Infof("reorg from %s to %s", curHead.String(), newHead.String())
			logSyncer.Errorf("unexpected error from ReorgDiff during log: %s", err.Error())
		}
	}
}

// widen computes a tipset implied by the input tipset and the store that
// could potentially be the heaviest tipset. In the context of EC, widen
// returns the union of the input tipset and the biggest tipset with the same
// parents from the store.
// TODO: this leaks EC abstractions into the syncer, we should think about this.
func (syncer *Syncer) widen(ctx context.Context, ts types.TipSet) (types.TipSet, error) {
	// Lookup tipsets with the same parents from the store.
	parentSet, err := ts.Parents()
	if err != nil {
		return types.UndefTipSet, err
	}
	height, err := ts.Height()
	if err != nil {
		return types.UndefTipSet, err
	}
	if !syncer.chainStore.HasTipSetAndStatesWithParentsAndHeight(parentSet, height) {
		return types.UndefTipSet, nil
	}
	candidates, err := syncer.chainStore.GetTipSetAndStatesByParentsAndHeight(parentSet, height)
	if err != nil {
		return types.UndefTipSet, err
	}
	if len(candidates) == 0 {
		return types.UndefTipSet, nil
	}

	// Only take the tipset with the most blocks (this is EC specific logic)
	max := candidates[0].TipSet
	for _, candidate := range candidates[0:] {
		if candidate.TipSet.Len() > max.Len() {
			max = candidate.TipSet
		}
	}

	// Form a new tipset from the union of ts and the largest in the store, de-duped.
	var blockSlice []*types.Block
	blockCids := make(map[cid.Cid]struct{})
	for i := 0; i < ts.Len(); i++ {
		blk := ts.At(i)
		blockCids[blk.Cid()] = struct{}{}
		blockSlice = append(blockSlice, blk)
	}
	for i := 0; i < max.Len(); i++ {
		blk := max.At(i)
		if _, found := blockCids[blk.Cid()]; !found {
			blockSlice = append(blockSlice, blk)
			blockCids[blk.Cid()] = struct{}{}
		}
	}
	wts, err := types.NewTipSet(blockSlice...)
	if err != nil {
		return types.UndefTipSet, err
	}

	// check that the tipset is distinct from the input and tipsets from the store.
	if wts.String() == ts.String() || wts.String() == max.String() {
		return types.UndefTipSet, nil
	}

	return wts, nil
}

// HandleNewTipSet extends the Syncer's chain store with the given tipset if they
// represent a valid extension. It limits the length of new chains it will
// attempt to validate and caches invalid blocks it has encountered to
// help prevent DOS.
func (syncer *Syncer) HandleNewTipSet(ctx context.Context, ci *types.ChainInfo, trusted bool) (err error) {
	logSyncer.Debugf("Begin fetch and sync of chain with head %v", ci.Head)
	ctx, span := trace.StartSpan(ctx, "Syncer.HandleNewTipSet")
	span.AddAttributes(trace.StringAttribute("tipset", ci.Head.String()))
	defer tracing.AddErrorEndSpan(ctx, span, &err)

	// This lock could last a long time as we fetch all the blocks needed to sync the chain.
	// This is justified because the app is pretty useless until it is synced.
	// It's better for multiple calls to wait here than to try to fetch the chain independently.
	syncer.mu.Lock()
	defer syncer.mu.Unlock()

	// If the store already has this tipset then the syncer is finished.
	if syncer.chainStore.HasTipSetAndState(ctx, ci.Head) {
		return nil
	}

	curHead, err := syncer.chainStore.GetTipSet(syncer.chainStore.GetHead())
	if err != nil {
		return err
	}
	curHeight, err := curHead.Height()
	if err != nil {
		return err
	}

	syncer.reporter.UpdateStatus(syncingStarted(syncer.clock.Now().Unix()), syncHead(ci.Head), syncHeight(ci.Height), syncTrusted(trusted), syncComplete(false))
	defer syncer.reporter.UpdateStatus(syncComplete(true))

	// If we do not trust the peer head check finality
	if !trusted && ExceedsUntrustedChainLength(curHeight, ci.Height) {
		return ErrNewChainTooLong
	}

	syncer.reporter.UpdateStatus(syncFetchComplete(false))
	chain, err := syncer.fetcher.FetchTipSets(ctx, ci.Head, ci.Peer, func(t types.TipSet) (bool, error) {
		parents, err := t.Parents()
		if err != nil {
			return true, err
		}
		height, err := t.Height()
		if err != nil {
			return false, err
		}

		// update status with latest fetched head and height
		syncer.reporter.UpdateStatus(fetchHead(t.Key()), fetchHeight(height))
		return syncer.chainStore.HasTipSetAndState(ctx, parents), nil
	})
	syncer.reporter.UpdateStatus(syncFetchComplete(true))
	if err != nil {
		return err
	}
	// Fetcher returns chain in Traversal order, reverse it to height order
	Reverse(chain)

	parent, grandParent, err := syncer.ancestorsFromStore(chain[0])
	if err != nil {
		return err
	}

	// Try adding the tipsets of the chain to the store, checking for new
	// heaviest tipsets.
	for i, ts := range chain {
		// TODO: this "i==0" leaks EC specifics into syncer abstraction
		// for the sake of efficiency, consider plugging up this leak.
		var wts types.TipSet
		if i == 0 {
			wts, err = syncer.widen(ctx, ts)
			if err != nil {
				return err
			}
			if wts.Defined() {
				logSyncer.Debug("attempt to sync after widen")
				err = syncer.syncOne(ctx, grandParent, parent, wts)
				if err != nil {
					return err
				}
			}
		}
		// If the chain has length greater than 1, then we need to sync each tipset
		// in the chain in order to process the chain fully, including the non-widened
		// first tipset.
		// If the chan has length == 1, we can avoid processing the non-widened tipset
		// as a performance optimization, because this tipset cannot be heavier
		// than the widened first tipset.
		if !wts.Defined() || len(chain) > 1 {
			err = syncer.syncOne(ctx, grandParent, parent, ts)
			if err != nil {
				// While `syncOne` can indeed fail for reasons other than consensus,
				// adding to the badTipSets at this point is the simplest, since we
				// have access to the chain. If syncOne fails for non-consensus reasons,
				// there is no assumption that the running node's data is valid at all,
				// so we don't really lose anything with this simplification.
				syncer.badTipSets.AddChain(chain[i:])
				return err
			}
		}
		if i%500 == 0 {
			logSyncer.Infof("processing block %d of %v for chain with head at %v", i, len(chain), ci.Head.String())
		}
		grandParent = parent
		parent = ts
	}
	return nil
}

// Status returns the current chain status.
func (syncer *Syncer) Status() Status {
	return syncer.reporter.Status()
}

// ExceedsUntrustedChainLength returns true if the delta between curHeight and newHeight
// exceeds the maximum number of blocks to accept if syncing without trust, false otherwise.
func ExceedsUntrustedChainLength(curHeight, newHeight uint64) bool {
	maxChainLength := curHeight + uint64(UntrustedChainHeightLimit)
	return newHeight > maxChainLength
}
