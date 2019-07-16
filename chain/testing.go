package chain

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-filecoin/address"
	"github.com/filecoin-project/go-filecoin/types"
)

// Builder builds fake chains and acts as a provider and fetcher for the chain thus generated.
// All blocks are unique (even if they share parents) and form valid chains of parents and heights,
// but do not carry valid tickets. Each block contributes a weight of 1.
// State root CIDs are computed by an abstract StateBuilder. The default FakeStateBuilder produces
// state CIDs that are distinct but not CIDs of any real state tree. A more sophisticated
// builder could actually apply the messages to a state tree (not yet implemented).
type Builder struct {
	t            *testing.T
	minerAddress address.Address
	stateBuilder StateBuilder
	seq          uint64 // For unique tickets

	blocks map[cid.Cid]*types.Block
}

var _ TipSetProvider = (*Builder)(nil)
var _ syncFetcher = (*Builder)(nil)

// NewBuilder builds a new chain faker.
// Blocks will have `miner` set as the miner address.
func NewBuilder(t *testing.T, miner address.Address) *Builder {
	return &Builder{
		t:            t,
		minerAddress: miner,
		stateBuilder: &FakeStateBuilder{},
		blocks:       make(map[cid.Cid]*types.Block),
	}
}

// AppendTo creates and returns a new block child of `parents`, with no messages.
func (f *Builder) AppendTo(parents ...*types.Block) *types.Block {
	tip := types.UndefTipSet
	if len(parents) > 0 {
		tip = types.RequireNewTipSet(f.t, parents...)
	}
	return f.BuildToTip(tip, nil)
}

// BuildTo creates and returns a new block child of singleton tipset `parent`. See BuildToTip.
func (f *Builder) BuildTo(parent *types.Block, build func(b *BlockBuilder)) *types.Block {
	tip := types.UndefTipSet
	if parent != nil {
		tip = types.RequireNewTipSet(f.t, parent)
	}
	return f.BuildToTip(tip, build)
}

// BuildToTip creates and returns anew block child of `parent`.
// The `build` function is invoked to modify the block before it is stored.
func (f *Builder) BuildToTip(parent types.TipSet, build func(b *BlockBuilder)) *types.Block {
	ticket := make([]byte, binary.Size(f.seq))
	binary.BigEndian.PutUint64(ticket, f.seq)
	f.seq++

	// Sum weight of parents' parent weight, plus one for each parent.
	parentWeight := types.Uint64(0)
	for i := 0; i < parent.Len(); i++ {
		parentWeight += parent.At(i).ParentWeight + 1
	}

	height := types.Uint64(0)
	if parent.Defined() {
		height = parent.At(0).Height + 1
	}

	b := &types.Block{
		Ticket:          ticket,
		Miner:           f.minerAddress,
		ParentWeight:    parentWeight,
		Parents:         parent.Key(),
		Height:          height,
		Messages:        []*types.SignedMessage{},
		MessageReceipts: []*types.MessageReceipt{},
		//StateRoot:       stateRoot,
		//Proof PoStProof `json:"proof"`
		//Timestamp Uint64 `json:"timestamp"`
	}
	// Nonce intentionally omitted as it will go away.

	if build != nil {
		build(&BlockBuilder{b})
	}

	// Compute state root from block.
	var err error
	b.StateRoot, err = f.stateBuilder.ComputeStateRoot(b)
	require.NoError(f.t, err)

	f.blocks[b.Cid()] = b
	return b

}

///// Block builder /////

// BlockBuilder mutates blocks as they are generated.
type BlockBuilder struct {
	block *types.Block
}

// SetTicket sets the block's ticket.
func (bb *BlockBuilder) SetTicket(ticket []byte) {
	bb.block.Ticket = ticket
}

// SetTimestamp sets the block's timestamp.
func (bb *BlockBuilder) SetTimestamp(timestamp types.Uint64) {
	bb.block.Timestamp = timestamp
}

// IncHeight increments the block's height, implying a number of null blocks before this one
// is mined.
func (bb *BlockBuilder) IncHeight(nullBlocks types.Uint64) {
	bb.block.Height += nullBlocks
}

// AddMessage adds a message & receipt to the block.
func (bb *BlockBuilder) AddMessage(msg *types.SignedMessage, rcpt *types.MessageReceipt) {
	bb.block.Messages = append(bb.block.Messages, msg)
	bb.block.MessageReceipts = append(bb.block.MessageReceipts, rcpt)
}

///// State builder /////

// StateBuilder abstracts the computation of state root CIDs from the chain builder.
type StateBuilder interface {
	ComputeStateRoot(block *types.Block) (cid.Cid, error)
}

// FakeStateBuilder computes a fake state CID by hashing the CIDs of a block's parents and messages.
type FakeStateBuilder struct {
}

// ComputeStateRoot computes a fake state root for `Block`.
func (FakeStateBuilder) ComputeStateRoot(block *types.Block) (cid.Cid, error) {
	var fakeState []cid.Cid
	for it := block.Parents.Iter(); !it.Complete(); it.Next() {
		fakeState = append(fakeState, it.Value())
	}
	for i := 0; i < len(block.Messages); i++ {
		mCid, err := block.Messages[i].Cid()
		if err != nil {
			return cid.Undef, err
		}
		fakeState = append(fakeState, mCid)
	}

	return makeCid(fakeState)
}

///// Interface implementations /////

// GetBlocks returns the blocks identified by `cids`.
func (f *Builder) GetBlocks(ctx context.Context, cids []cid.Cid) ([]*types.Block, error) {
	var ret []*types.Block
	for _, c := range cids {
		if block, ok := f.blocks[c]; ok {
			ret = append(ret, block)
		} else {
			return nil, fmt.Errorf("no block %s", c)
		}
	}
	return ret, nil
}

// GetTipSet returns the tipset identified by `key`.
func (f *Builder) GetTipSet(key types.TipSetKey) (types.TipSet, error) {
	var blocks []*types.Block
	for it := key.Iter(); !it.Complete(); it.Next() {
		if block, ok := f.blocks[it.Value()]; ok {
			blocks = append(blocks, block)
		} else {
			return types.UndefTipSet, fmt.Errorf("no block %s", it.Value())
		}

	}
	return types.NewTipSet(blocks...)
}

///// Internals /////

func makeCid(i interface{}) (cid.Cid, error) {
	bytes, err := cbor.DumpObject(i)
	if err != nil {
		return cid.Undef, err
	}
	return cid.Prefix{
		Version:  1,
		Codec:    cid.DagCBOR,
		MhType:   types.DefaultHashFunction,
		MhLength: -1,
	}.Sum(bytes)
}
