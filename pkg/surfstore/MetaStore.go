package surfstore

import (
	context "context"
	"errors"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	lock               sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.lock.Lock()
	currentMetaData, exists := m.FileMetaMap[fileMetaData.Filename]
	if exists && currentMetaData.Version+1 != fileMetaData.Version {
		m.lock.Unlock()
		return &Version{Version: -1}, errors.New("wrong version number")
	} else {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		m.lock.Unlock()
		return &Version{Version: fileMetaData.Version}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := make(map[string]*BlockHashes)
	for _, hash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(hash)
		if _, exist := blockStoreMap[server]; !exist {
			blockStoreMap[server] = &BlockHashes{Hashes: []string{}}
		}
		blockStoreMap[server].Hashes = append(blockStoreMap[server].Hashes, hash)
	}
	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
