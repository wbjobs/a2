package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"rs-service/internal/erasure"
	"sync"
)

type Store struct {
	nodesBaseDir string
	nodePaths    []string
	mu           sync.RWMutex
}

func NewStore(baseDir string) (*Store, error) {
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		absBase = baseDir
	}

	nodePaths := make([]string, 9)
	for i := 0; i < 9; i++ {
		nodePath := filepath.Join(absBase, fmt.Sprintf("node_%d", i))
		if err := os.MkdirAll(nodePath, 0755); err != nil {
			return nil, fmt.Errorf("create node dir %s: %w", nodePath, err)
		}
		nodePaths[i] = nodePath
	}

	return &Store{
		nodesBaseDir: absBase,
		nodePaths:    nodePaths,
	}, nil
}

func (s *Store) GetNodePath(nodeID int) (string, error) {
	if nodeID < 0 || nodeID >= len(s.nodePaths) {
		return "", fmt.Errorf("invalid node id: %d", nodeID)
	}
	return s.nodePaths[nodeID], nil
}

func (s *Store) GetShardPath(nodeID int, fileID string, shardIndex int) (string, error) {
	nodePath, err := s.GetNodePath(nodeID)
	if err != nil {
		return "", err
	}
	shardName := fmt.Sprintf("%s_shard_%d", fileID, shardIndex)
	return filepath.Join(nodePath, shardName), nil
}

func (s *Store) SaveShard(nodeID int, fileID string, shardIndex int, data []byte) (string, int64, error) {
	shardPath, err := s.GetShardPath(nodeID, fileID, shardIndex)
	if err != nil {
		return "", 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.WriteFile(shardPath, data, 0644); err != nil {
		return "", 0, fmt.Errorf("write shard %s: %w", shardPath, err)
	}

	hash := erasure.HashData(data)
	size := int64(len(data))

	return hash, size, nil
}

func (s *Store) ReadShard(nodeID int, fileID string, shardIndex int) ([]byte, string, error) {
	shardPath, err := s.GetShardPath(nodeID, fileID, shardIndex)
	if err != nil {
		return nil, "", err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(shardPath)
	if err != nil {
		return nil, "", fmt.Errorf("read shard %s: %w", shardPath, err)
	}

	hash := erasure.HashData(data)
	return data, hash, nil
}

func (s *Store) DeleteShard(nodeID int, fileID string, shardIndex int) error {
	shardPath, err := s.GetShardPath(nodeID, fileID, shardIndex)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.Remove(shardPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("delete shard %s: %w", shardPath, err)
		}
	}
	return nil
}

func (s *Store) ShardExists(nodeID int, fileID string, shardIndex int) (bool, error) {
	shardPath, err := s.GetShardPath(nodeID, fileID, shardIndex)
	if err != nil {
		return false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	_, err = os.Stat(shardPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("stat shard %s: %w", shardPath, err)
}

func (s *Store) SaveShards(fileID string, shards [][]byte, nodeIDs []int) ([]string, []int64, error) {
	if len(shards) != len(nodeIDs) {
		return nil, nil, fmt.Errorf("shards count %d != node ids count %d", len(shards), len(nodeIDs))
	}

	hashes := make([]string, len(shards))
	sizes := make([]int64, len(shards))

	for i, shard := range shards {
		hash, size, err := s.SaveShard(nodeIDs[i], fileID, i, shard)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = s.DeleteShard(nodeIDs[j], fileID, j)
			}
			return nil, nil, err
		}
		hashes[i] = hash
		sizes[i] = size
	}

	return hashes, sizes, nil
}

func (s *Store) DeleteFileShards(fileID string, nodeIDs []int, shardCount int) error {
	var firstErr error
	for i := 0; i < shardCount; i++ {
		if err := s.DeleteShard(nodeIDs[i], fileID, i); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
