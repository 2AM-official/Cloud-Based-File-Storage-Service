package surfstore

import (
	context "context"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
	// mutex
	mu sync.Mutex
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	b := bs.BlockMap[blockHash.Hash]
	return b, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	bHash := GetBlockHashString(block.BlockData)
	bs.BlockMap[bHash] = block
	//TODO: how to judge the true or false of the Success
	return &Success{
		Flag: true,
	}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	var h []string
	for i := 0; i < len(blockHashesIn.Hashes); i++ {
		_, found := bs.BlockMap[blockHashesIn.Hashes[i]]
		if found {
			h = append(h, blockHashesIn.Hashes[i])
		}
	}
	return &BlockHashes{
		Hashes: h,
	}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
