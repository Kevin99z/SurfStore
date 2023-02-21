package surfstore

import (
	context "context"
	"errors"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block //not thread safe
	lock     sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.lock.Lock()
	if data, ok := bs.BlockMap[blockHash.Hash]; ok {
		bs.lock.Unlock()
		return data, nil
	} else {
		bs.lock.Unlock()
		return nil, errors.New("block not exist")
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.BlockData)
	bs.lock.Lock()
	bs.BlockMap[hash] = block
	bs.lock.Unlock()
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	res := make([]string, 0)
	for _, hash := range blockHashesIn.Hashes {
		bs.lock.Lock()
		if _, ok := bs.BlockMap[hash]; ok {
			res = append(res, hash)
		}
		bs.lock.Unlock()
	}
	return &BlockHashes{Hashes: res}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
