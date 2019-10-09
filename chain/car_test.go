package chain_test

import (
	"bufio"
	"bytes"
	"context"
	"testing"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-filecoin/address"
	"github.com/filecoin-project/go-filecoin/chain"
	tf "github.com/filecoin-project/go-filecoin/testhelpers/testflags"
	"github.com/filecoin-project/go-filecoin/types"
)

func TestChainImportExportSimple(t *testing.T) {
	tf.UnitTest(t)
	ctx := context.Background()

	cb := chain.NewBuilder(t, address.Undef)

	gene := cb.NewGenesis()
	ts10 := cb.AppendManyOn(9, gene)

	var buf bytes.Buffer
	carW := bufio.NewWriter(&buf)

	// export the car file to a buffer
	exportedKey, err := chain.Export(ctx, ts10, cb, cb, carW)
	assert.NoError(t, err)
	assert.Equal(t, ts10.Key(), exportedKey)
	require.NoError(t, carW.Flush())

	mds := ds.NewMapDatastore()
	bstore := blockstore.NewBlockstore(mds)

	// import the car file from the buffer
	carR := bufio.NewReader(&buf)
	importedKey, err := chain.Import(ctx, bstore, carR)
	assert.NoError(t, err)
	assert.Equal(t, ts10.Key(), importedKey)

	// walk the blockstore and assert it had all blocks imported
	validateBlockstoreImport(t, ts10.Key(), gene.Key(), bstore)

	t.Run("failes for tipset not in store", func(t *testing.T) {
		cb := chain.NewBuilder(t, address.Undef)
		var buf bytes.Buffer
		carW := bufio.NewWriter(&buf)
		_, err = chain.Export(ctx, ts10, cb, cb, carW)
		assert.Error(t, err)

	})
}

func TestChainImportExportMessages(t *testing.T) {
	tf.UnitTest(t)
	ctx := context.Background()

	keys := types.MustGenerateKeyInfo(1, 42)
	mm := types.NewMessageMaker(t, keys)
	alice := mm.Addresses()[0]
	cb := chain.NewBuilder(t, address.Undef)

	gene := cb.NewGenesis()
	ts10 := cb.AppendManyOn(9, gene)
	msgs := []*types.SignedMessage{
		mm.NewSignedMessage(alice, 1),
		mm.NewSignedMessage(alice, 2),
		mm.NewSignedMessage(alice, 3),
		mm.NewSignedMessage(alice, 4),
		mm.NewSignedMessage(alice, 5),
	}
	rcts := types.EmptyReceipts(5)
	ts11 := cb.BuildOneOn(ts10, func(b *chain.BlockBuilder) {
		b.AddMessages(
			msgs,
			rcts,
		)
	})

	var buf bytes.Buffer
	carW := bufio.NewWriter(&buf)

	// export the car file to a buffer
	exportedKey, err := chain.Export(ctx, ts11, cb, cb, carW)
	assert.NoError(t, err)
	assert.Equal(t, ts11.Key(), exportedKey)
	require.NoError(t, carW.Flush())

	mds := ds.NewMapDatastore()
	bstore := blockstore.NewBlockstore(mds)

	// import the car file from the buffer
	carR := bufio.NewReader(&buf)
	importedKey, err := chain.Import(ctx, bstore, carR)
	assert.NoError(t, err)
	assert.Equal(t, ts11.Key(), importedKey)

	// walk the blockstore and assert it had all blocks imported
	validateBlockstoreImport(t, ts11.Key(), gene.Key(), bstore)
}

func TestChainImportExportMultiTipSetWithMessages(t *testing.T) {
	tf.UnitTest(t)
	ctx := context.Background()

	keys := types.MustGenerateKeyInfo(1, 42)
	mm := types.NewMessageMaker(t, keys)
	alice := mm.Addresses()[0]
	cb := chain.NewBuilder(t, address.Undef)

	gene := cb.NewGenesis()
	ts10 := cb.AppendManyOn(9, gene)
	msgs := []*types.SignedMessage{
		mm.NewSignedMessage(alice, 1),
		mm.NewSignedMessage(alice, 2),
		mm.NewSignedMessage(alice, 3),
		mm.NewSignedMessage(alice, 4),
		mm.NewSignedMessage(alice, 5),
	}
	rcts := types.EmptyReceipts(5)
	ts11 := cb.BuildOneOn(ts10, func(b *chain.BlockBuilder) {
		b.AddMessages(
			msgs,
			rcts,
		)
	})

	ts12 := cb.AppendOn(ts11, 3)

	var buf bytes.Buffer
	carW := bufio.NewWriter(&buf)

	// export the car file to a buffer
	exportedKey, err := chain.Export(ctx, ts12, cb, cb, carW)
	assert.NoError(t, err)
	assert.Equal(t, ts12.Key(), exportedKey)
	require.NoError(t, carW.Flush())

	mds := ds.NewMapDatastore()
	bstore := blockstore.NewBlockstore(mds)

	// import the car file from the buffer
	carR := bufio.NewReader(&buf)
	importedKey, err := chain.Import(ctx, bstore, carR)
	assert.NoError(t, err)
	assert.Equal(t, ts12.Key(), importedKey)

	// walk the blockstore and assert it had all blocks imported
	validateBlockstoreImport(t, ts12.Key(), gene.Key(), bstore)
}

func validateBlockstoreImport(t *testing.T, start, stop types.TipSetKey, bstore blockstore.Blockstore) {
	// walk the blockstore and assert it had all blocks imported
	cur := start
	for !cur.Equals(stop) {
		parents := cid.NewSet()
		for _, c := range cur.ToSlice() {
			bsBlk, err := bstore.Get(c)
			assert.NoError(t, err)
			blk, err := types.DecodeBlock(bsBlk.RawData())
			assert.NoError(t, err)

			if !blk.Messages.Equals(types.EmptyMessagesCID) {
				bsMsgs, err := bstore.Get(blk.Messages)
				assert.NoError(t, err)
				_, err = types.DecodeMessages(bsMsgs.RawData())
				assert.NoError(t, err)
			}

			if !blk.MessageReceipts.Equals(types.EmptyReceiptsCID) {
				bsRcts, err := bstore.Get(blk.MessageReceipts)
				assert.NoError(t, err)
				_, err = types.DecodeReceipts(bsRcts.RawData())
				assert.NoError(t, err)
			}

			for _, p := range blk.Parents.ToSlice() {
				parents.Add(p)
			}
		}
		cur = types.NewTipSetKey(parents.Keys()...)
	}
}
