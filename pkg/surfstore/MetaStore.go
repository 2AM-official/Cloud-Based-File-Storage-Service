package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"

	"sync"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	// implement meta store server
	UnimplementedMetaStoreServer

	// mutex
	mu sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// check whether the file is in the FileMetaMap
	fName := fileMetaData.Filename
	fVersion := fileMetaData.Version
	_, inServer := m.FileMetaMap[fName]
	var curVersion int32
	if !inServer {
		curVersion = 0
	} else {
		curVersion = m.FileMetaMap[fName].Version
	}

	//TODO: is this check satisfies the demand?
	if fVersion != curVersion+1 {
		return &Version{
			Version: -1,
		}, nil
	} else {
		m.FileMetaMap[fName] = fileMetaData
		return &Version{
			Version: fVersion,
		}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
// defined by ourselves
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
