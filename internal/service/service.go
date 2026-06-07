package service

import (
	"bytes"
	"fmt"
	"io"
	"rs-service/internal/db"
	"rs-service/internal/erasure"
	"rs-service/internal/model"
	"rs-service/internal/storage"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	enc   *erasure.Encoder
	store *storage.Store
}

func NewService(enc *erasure.Encoder, store *storage.Store) *Service {
	return &Service{
		enc:   enc,
		store: store,
	}
}

type UploadResult struct {
	FileID       string `json:"file_id"`
	FileName     string `json:"file_name"`
	FileSize     int64  `json:"file_size"`
	OriginalHash string `json:"original_hash"`
	ShardSize    int64  `json:"shard_size"`
	DataShards   int    `json:"data_shards"`
	ParityShards int    `json:"parity_shards"`
}

func (s *Service) UploadFile(fileName string, r io.Reader, fileSize int64) (*UploadResult, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	if int64(len(data)) != fileSize {
		return nil, fmt.Errorf("file size mismatch: read %d, expected %d", len(data), fileSize)
	}

	originalHash := erasure.HashData(data)

	shards, shardSize, err := s.enc.Encode(data)
	if err != nil {
		return nil, fmt.Errorf("encode file: %w", err)
	}

	onlineNodes, err := db.GetOnlineNodeIDs()
	if err != nil {
		return nil, fmt.Errorf("get online nodes: %w", err)
	}
	if len(onlineNodes) < erasure.TotalShards {
		return nil, fmt.Errorf("not enough online nodes: need %d, have %d", erasure.TotalShards, len(onlineNodes))
	}

	nodeIDs := onlineNodes[:erasure.TotalShards]
	fileID := uuid.New().String()

	hashes, sizes, err := s.store.SaveShards(fileID, shards, nodeIDs)
	if err != nil {
		return nil, fmt.Errorf("save shards: %w", err)
	}

	fileMeta := &model.File{
		ID:           fileID,
		Name:         fileName,
		Size:         fileSize,
		OriginalHash: originalHash,
		ShardSize:    shardSize,
		DataShards:   erasure.DataShards,
		ParityShards: erasure.ParityShards,
		TotalShards:  erasure.TotalShards,
		CreatedAt:    time.Now(),
	}
	if err := db.InsertFile(fileMeta); err != nil {
		_ = s.store.DeleteFileShards(fileID, nodeIDs, erasure.TotalShards)
		return nil, fmt.Errorf("save file meta: %w", err)
	}

	for i := range shards {
		shard := &model.Shard{
			FileID:     fileID,
			ShardIndex: i,
			NodeID:     nodeIDs[i],
			Size:       sizes[i],
			Hash:       hashes[i],
			IsParity:   i >= erasure.DataShards,
			StoredAt:   time.Now(),
		}
		if err := db.InsertShard(shard); err != nil {
			_ = s.store.DeleteFileShards(fileID, nodeIDs, erasure.TotalShards)
			return nil, fmt.Errorf("save shard meta: %w", err)
		}
	}

	return &UploadResult{
		FileID:       fileID,
		FileName:     fileName,
		FileSize:     fileSize,
		OriginalHash: originalHash,
		ShardSize:    shardSize,
		DataShards:   erasure.DataShards,
		ParityShards: erasure.ParityShards,
	}, nil
}

type RebuildResult struct {
	FileID          string   `json:"file_id"`
	FileName        string   `json:"file_name"`
	FailedNodeIDs   []int    `json:"failed_node_ids"`
	StartTime       time.Time `json:"start_time"`
	EndTime         time.Time `json:"end_time"`
	DurationMs      int64    `json:"duration_ms"`
	DataSize        int64    `json:"data_size"`
	HashVerified    bool     `json:"hash_verified"`
	OriginalHash    string   `json:"original_hash"`
	RebuiltHash     string   `json:"rebuilt_hash,omitempty"`
	RebuiltShards   []int    `json:"rebuilt_shards"`
	Status          string   `json:"status"`
	ErrorMessage    string   `json:"error_message,omitempty"`
}

func (s *Service) RebuildFile(fileID string, failedNodeIDs []int) (*RebuildResult, error) {
	startTime := time.Now()
	result := &RebuildResult{
		FileID:        fileID,
		FailedNodeIDs: failedNodeIDs,
		StartTime:     startTime,
	}

	fileMeta, err := db.GetFile(fileID)
	if err != nil {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("get file meta: %v", err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		s.saveRebuildLog(result)
		return result, nil
	}

	result.FileName = fileMeta.Name
	result.OriginalHash = fileMeta.OriginalHash
	result.DataSize = fileMeta.Size

	shardsMeta, err := db.GetShardsByFile(fileID)
	if err != nil {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("get shards meta: %v", err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		s.saveRebuildLog(result)
		return result, nil
	}

	failedNodeMap := make(map[int]bool)
	for _, id := range failedNodeIDs {
		failedNodeMap[id] = true
	}

	shards := make([][]byte, erasure.TotalShards)
	present := make([]bool, erasure.TotalShards)
	var rebuiltShardIndices []int

	for _, sm := range shardsMeta {
		if failedNodeMap[sm.NodeID] {
			shards[sm.ShardIndex] = nil
			present[sm.ShardIndex] = false
			rebuiltShardIndices = append(rebuiltShardIndices, sm.ShardIndex)
		} else {
			data, hash, err := s.store.ReadShard(sm.NodeID, fileID, sm.ShardIndex)
			if err != nil || hash != sm.Hash {
				shards[sm.ShardIndex] = nil
				present[sm.ShardIndex] = false
				rebuiltShardIndices = append(rebuiltShardIndices, sm.ShardIndex)
				failedNodeMap[sm.NodeID] = true
			} else {
				shards[sm.ShardIndex] = data
				present[sm.ShardIndex] = true
			}
		}
	}

	missingCount := 0
	for _, p := range present {
		if !p {
			missingCount++
		}
	}
	if missingCount > erasure.ParityShards {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("too many missing shards: %d, max recoverable: %d", missingCount, erasure.ParityShards)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		result.RebuiltShards = rebuiltShardIndices
		s.saveRebuildLog(result)
		return result, nil
	}

	if err := s.enc.Reconstruct(shards, present); err != nil {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("reconstruct shards: %v", err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		result.RebuiltShards = rebuiltShardIndices
		s.saveRebuildLog(result)
		return result, nil
	}

	ok, err := s.enc.Verify(shards)
	if err != nil || !ok {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("verify shards failed: ok=%v, err=%v", ok, err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		result.RebuiltShards = rebuiltShardIndices
		s.saveRebuildLog(result)
		return result, nil
	}

	rebuiltData, err := s.enc.Join(shards, fileMeta.Size)
	if err != nil {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("join shards: %v", err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		result.RebuiltShards = rebuiltShardIndices
		s.saveRebuildLog(result)
		return result, nil
	}

	rebuiltHash := erasure.HashData(rebuiltData)
	hashVerified := rebuiltHash == fileMeta.OriginalHash
	result.RebuiltHash = rebuiltHash
	result.HashVerified = hashVerified

	if !hashVerified {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = "hash verification failed"
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		result.RebuiltShards = rebuiltShardIndices
		s.saveRebuildLog(result)
		return result, nil
	}

	allNodes, err := db.GetOnlineNodeIDs()
	if err != nil {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("get online nodes: %v", err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		result.RebuiltShards = rebuiltShardIndices
		s.saveRebuildLog(result)
		return result, nil
	}

	usedNodes := make(map[int]bool)
	for _, sm := range shardsMeta {
		usedNodes[sm.NodeID] = true
	}

	availableNodes := make([]int, 0)
	for _, id := range allNodes {
		if !failedNodeMap[id] && !usedNodes[id] {
			availableNodes = append(availableNodes, id)
		}
	}
	sort.Ints(availableNodes)

	nodeIdx := 0
	for _, idx := range rebuiltShardIndices {
		shardMeta := shardsMeta[idx]
		newNodeID := shardMeta.NodeID

		if failedNodeMap[shardMeta.NodeID] {
			if nodeIdx < len(availableNodes) {
				newNodeID = availableNodes[nodeIdx]
				nodeIdx++
			}
		}

		hash, size, err := s.store.SaveShard(newNodeID, fileID, idx, shards[idx])
		if err != nil {
			result.Status = db.RebuildStatusFailed
			result.ErrorMessage = fmt.Sprintf("save rebuilt shard %d: %v", idx, err)
			result.EndTime = time.Now()
			result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
			result.RebuiltShards = rebuiltShardIndices
			s.saveRebuildLog(result)
			return result, nil
		}

		if newNodeID != shardMeta.NodeID {
			if err := db.UpdateShardNode(fileID, idx, newNodeID); err != nil {
				result.Status = db.RebuildStatusFailed
				result.ErrorMessage = fmt.Sprintf("update shard node: %v", err)
				result.EndTime = time.Now()
				result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
				result.RebuiltShards = rebuiltShardIndices
				s.saveRebuildLog(result)
				return result, nil
			}
			_ = s.store.DeleteShard(shardMeta.NodeID, fileID, idx)
		}

		if err := db.UpdateShardHashAndSize(fileID, idx, hash, size); err != nil {
			result.Status = db.RebuildStatusFailed
			result.ErrorMessage = fmt.Sprintf("update shard hash: %v", err)
			result.EndTime = time.Now()
			result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
			result.RebuiltShards = rebuiltShardIndices
			s.saveRebuildLog(result)
			return result, nil
		}
	}

	result.Status = db.RebuildStatusSuccess
	result.EndTime = time.Now()
	result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
	result.RebuiltShards = rebuiltShardIndices
	s.saveRebuildLog(result)

	return result, nil
}

func (s *Service) saveRebuildLog(result *RebuildResult) {
	failedNodeIDsStr := intsToString(result.FailedNodeIDs)
	rebuiltShardsStr := intsToString(result.RebuiltShards)

	log := &model.RebuildLog{
		FileID:        result.FileID,
		FailedNodeIDs: failedNodeIDsStr,
		StartTime:     result.StartTime,
		EndTime:       result.EndTime,
		DurationMs:    result.DurationMs,
		DataSize:      result.DataSize,
		HashVerified:  result.HashVerified,
		RebuiltShards: rebuiltShardsStr,
		Status:        result.Status,
		ErrorMessage:  result.ErrorMessage,
	}
	_ = db.InsertRebuildLog(log)
}

func (s *Service) DownloadFile(fileID string) (string, []byte, error) {
	fileMeta, err := db.GetFile(fileID)
	if err != nil {
		return "", nil, fmt.Errorf("get file meta: %w", err)
	}

	shardsMeta, err := db.GetShardsByFile(fileID)
	if err != nil {
		return "", nil, fmt.Errorf("get shards meta: %w", err)
	}

	shards := make([][]byte, erasure.TotalShards)
	present := make([]bool, erasure.TotalShards)
	missingCount := 0

	for _, sm := range shardsMeta {
		node, err := db.GetNode(sm.NodeID)
		if err != nil || node.Status == db.NodeStatusOffline {
			shards[sm.ShardIndex] = nil
			present[sm.ShardIndex] = false
			missingCount++
			continue
		}

		data, hash, err := s.store.ReadShard(sm.NodeID, fileID, sm.ShardIndex)
		if err != nil || hash != sm.Hash {
			shards[sm.ShardIndex] = nil
			present[sm.ShardIndex] = false
			missingCount++
		} else {
			shards[sm.ShardIndex] = data
			present[sm.ShardIndex] = true
		}
	}

	if missingCount > 0 {
		if missingCount > erasure.ParityShards {
			return "", nil, fmt.Errorf("too many missing shards: %d, max recoverable: %d", missingCount, erasure.ParityShards)
		}

		if err := s.enc.ReconstructData(shards, present); err != nil {
			return "", nil, fmt.Errorf("reconstruct data: %w", err)
		}
	}

	data, err := s.enc.Join(shards, fileMeta.Size)
	if err != nil {
		return "", nil, fmt.Errorf("join shards: %w", err)
	}

	return fileMeta.Name, data, nil
}

func (s *Service) GetFileReader(fileID string) (string, io.ReadCloser, int64, error) {
	name, data, err := s.DownloadFile(fileID)
	if err != nil {
		return "", nil, 0, err
	}
	return name, io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func intsToString(ids []int) string {
	if len(ids) == 0 {
		return ""
	}
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(strs, ",")
}

func (s *Service) GetFilesAffectedByNode(nodeID int) ([]string, error) {
	return db.GetFilesOnNode(nodeID)
}
