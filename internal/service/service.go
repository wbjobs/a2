package service

import (
	"bytes"
	"fmt"
	"io"
	"rs-service/internal/db"
	"rs-service/internal/erasure"
	"rs-service/internal/metrics"
	"rs-service/internal/model"
	"rs-service/internal/storage"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	store *storage.Store
	mu    sync.Mutex
}

func NewService(store *storage.Store) *Service {
	return &Service{
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
	TotalShards  int    `json:"total_shards"`
	CodecName    string `json:"codec_name"`
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

	config := erasure.SelectConfig(fileSize)

	enc, err := erasure.GetEncoder(config.DataShards, config.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("get encoder: %w", err)
	}

	shards, shardSize, err := enc.Encode(data)
	if err != nil {
		return nil, fmt.Errorf("encode file: %w", err)
	}

	onlineNodes, err := db.GetOnlineNodeIDs()
	if err != nil {
		return nil, fmt.Errorf("get online nodes: %w", err)
	}
	if len(onlineNodes) < config.TotalShards {
		return nil, fmt.Errorf("not enough online nodes: need %d, have %d", config.TotalShards, len(onlineNodes))
	}

	nodeIDs := onlineNodes[:config.TotalShards]
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
		DataShards:   config.DataShards,
		ParityShards: config.ParityShards,
		TotalShards:  config.TotalShards,
		CreatedAt:    time.Now(),
	}
	if err := db.InsertFile(fileMeta); err != nil {
		_ = s.store.DeleteFileShards(fileID, nodeIDs, config.TotalShards)
		return nil, fmt.Errorf("save file meta: %w", err)
	}

	for i := range shards {
		shard := &model.Shard{
			FileID:     fileID,
			ShardIndex: i,
			NodeID:     nodeIDs[i],
			Size:       sizes[i],
			Hash:       hashes[i],
			IsParity:   i >= config.DataShards,
			StoredAt:   time.Now(),
		}
		if err := db.InsertShard(shard); err != nil {
			_ = s.store.DeleteFileShards(fileID, nodeIDs, config.TotalShards)
			return nil, fmt.Errorf("save shard meta: %w", err)
		}
	}

	go metrics.Get().RefreshFileStats()

	return &UploadResult{
		FileID:       fileID,
		FileName:     fileName,
		FileSize:     fileSize,
		OriginalHash: originalHash,
		ShardSize:    shardSize,
		DataShards:   config.DataShards,
		ParityShards: config.ParityShards,
		TotalShards:  config.TotalShards,
		CodecName:    config.Name,
	}, nil
}

type RebuildResult struct {
	FileID          string    `json:"file_id"`
	FileName        string    `json:"file_name"`
	FailedNodeIDs   []int     `json:"failed_node_ids"`
	StartTime       time.Time `json:"start_time"`
	EndTime         time.Time `json:"end_time"`
	DurationMs      int64     `json:"duration_ms"`
	DataSize        int64     `json:"data_size"`
	HashVerified    bool      `json:"hash_verified"`
	OriginalHash    string    `json:"original_hash"`
	RebuiltHash     string    `json:"rebuilt_hash,omitempty"`
	RebuiltShards   []int     `json:"rebuilt_shards"`
	Status          string    `json:"status"`
	ErrorMessage    string    `json:"error_message,omitempty"`
	CodecName       string    `json:"codec_name,omitempty"`
	IsLazyRebuild   bool      `json:"is_lazy_rebuild,omitempty"`
}

func (s *Service) RebuildFile(fileID string, failedNodeIDs []int) (*RebuildResult, error) {
	return s.rebuildFileInternal(fileID, failedNodeIDs, false)
}

func (s *Service) RebuildFileLazy(fileID string, failedNodeIDs []int) (*RebuildResult, error) {
	return s.rebuildFileInternal(fileID, failedNodeIDs, true)
}

func (s *Service) rebuildFileInternal(fileID string, failedNodeIDs []int, isLazy bool) (*RebuildResult, error) {
	startTime := time.Now()
	metrics.Get().RecordRebuildStart()

	result := &RebuildResult{
		FileID:        fileID,
		FailedNodeIDs: failedNodeIDs,
		StartTime:     startTime,
		IsLazyRebuild: isLazy,
	}

	fileMeta, err := db.GetFile(fileID)
	if err != nil {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("get file meta: %v", err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		s.saveRebuildLog(result)
		metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
		return result, nil
	}

	config, err := erasure.GetConfig(fileMeta.DataShards, fileMeta.ParityShards)
	if err != nil {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("get codec config: %v", err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		s.saveRebuildLog(result)
		metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
		return result, nil
	}

	result.CodecName = config.Name

	if len(failedNodeIDs) > config.ParityShards {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("too many failed nodes: %d, max recoverable: %d for %s",
			len(failedNodeIDs), config.ParityShards, config.Name)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		s.saveRebuildLog(result)
		metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
		return result, fmt.Errorf(result.ErrorMessage)
	}

	result.FileName = fileMeta.Name
	result.OriginalHash = fileMeta.OriginalHash
	result.DataSize = fileMeta.Size

	enc, err := erasure.GetEncoder(config.DataShards, config.ParityShards)
	if err != nil {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("get encoder: %v", err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		s.saveRebuildLog(result)
		metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
		return result, nil
	}

	shardsMeta, err := db.GetShardsByFile(fileID)
	if err != nil {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("get shards meta: %v", err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		s.saveRebuildLog(result)
		metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
		return result, nil
	}

	failedNodeMap := make(map[int]bool)
	for _, id := range failedNodeIDs {
		failedNodeMap[id] = true
	}

	shards := make([][]byte, config.TotalShards)
	present := make([]bool, config.TotalShards)
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
	if missingCount > config.ParityShards {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("too many missing shards: %d, max recoverable: %d for %s",
			missingCount, config.ParityShards, config.Name)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		result.RebuiltShards = rebuiltShardIndices
		s.saveRebuildLog(result)
		metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
		return result, nil
	}

	if err := enc.Reconstruct(shards, present); err != nil {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("reconstruct shards: %v", err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		result.RebuiltShards = rebuiltShardIndices
		s.saveRebuildLog(result)
		metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
		return result, nil
	}

	ok, err := enc.Verify(shards)
	if err != nil || !ok {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("verify shards failed: ok=%v, err=%v", ok, err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		result.RebuiltShards = rebuiltShardIndices
		s.saveRebuildLog(result)
		metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
		return result, nil
	}

	rebuiltData, err := enc.Join(shards, fileMeta.Size)
	if err != nil {
		result.Status = db.RebuildStatusFailed
		result.ErrorMessage = fmt.Sprintf("join shards: %v", err)
		result.EndTime = time.Now()
		result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
		result.RebuiltShards = rebuiltShardIndices
		s.saveRebuildLog(result)
		metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
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
		metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
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
		metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
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
			metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
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
				metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
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
			metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), false, isLazy)
			return result, nil
		}
	}

	result.Status = db.RebuildStatusSuccess
	result.EndTime = time.Now()
	result.DurationMs = result.EndTime.Sub(result.StartTime).Milliseconds()
	result.RebuiltShards = rebuiltShardIndices
	s.saveRebuildLog(result)
	metrics.Get().RecordRebuildComplete(result.EndTime.Sub(result.StartTime), true, isLazy)

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

func (s *Service) DownloadFile(fileID string) (string, []byte, *RebuildResult, error) {
	fileMeta, err := db.GetFile(fileID)
	if err != nil {
		return "", nil, nil, fmt.Errorf("get file meta: %w", err)
	}

	config, err := erasure.GetConfig(fileMeta.DataShards, fileMeta.ParityShards)
	if err != nil {
		return "", nil, nil, fmt.Errorf("get codec config: %w", err)
	}

	enc, err := erasure.GetEncoder(config.DataShards, config.ParityShards)
	if err != nil {
		return "", nil, nil, fmt.Errorf("get encoder: %w", err)
	}

	shardsMeta, err := db.GetShardsByFile(fileID)
	if err != nil {
		return "", nil, nil, fmt.Errorf("get shards meta: %w", err)
	}

	shards := make([][]byte, config.TotalShards)
	present := make([]bool, config.TotalShards)
	missingCount := 0
	var failedNodeIDs []int
	failedNodeMap := make(map[int]bool)

	for _, sm := range shardsMeta {
		node, err := db.GetNode(sm.NodeID)
		if err != nil || node.Status == db.NodeStatusOffline {
			shards[sm.ShardIndex] = nil
			present[sm.ShardIndex] = false
			missingCount++
			if !failedNodeMap[sm.NodeID] {
				failedNodeIDs = append(failedNodeIDs, sm.NodeID)
				failedNodeMap[sm.NodeID] = true
			}
			continue
		}

		data, hash, err := s.store.ReadShard(sm.NodeID, fileID, sm.ShardIndex)
		if err != nil || hash != sm.Hash {
			shards[sm.ShardIndex] = nil
			present[sm.ShardIndex] = false
			missingCount++
			if !failedNodeMap[sm.NodeID] {
				failedNodeIDs = append(failedNodeIDs, sm.NodeID)
				failedNodeMap[sm.NodeID] = true
			}
		} else {
			shards[sm.ShardIndex] = data
			present[sm.ShardIndex] = true
		}
	}

	var rebuildResult *RebuildResult
	if missingCount > 0 {
		if missingCount > config.ParityShards {
			return "", nil, nil, fmt.Errorf("too many missing shards: %d, max recoverable: %d for %s",
				missingCount, config.ParityShards, config.Name)
		}

		sort.Ints(failedNodeIDs)
		rebuildResult, err = s.RebuildFileLazy(fileID, failedNodeIDs)
		if err != nil {
			return "", nil, rebuildResult, fmt.Errorf("lazy rebuild failed: %w", err)
		}
		if rebuildResult.Status != db.RebuildStatusSuccess {
			return "", nil, rebuildResult, fmt.Errorf("lazy rebuild failed: %s", rebuildResult.ErrorMessage)
		}

		for _, sm := range shardsMeta {
			if shards[sm.ShardIndex] == nil {
				data, hash, err := s.store.ReadShard(sm.NodeID, fileID, sm.ShardIndex)
				if err != nil || hash != sm.Hash {
					shards[sm.ShardIndex] = nil
				} else {
					shards[sm.ShardIndex] = data
					present[sm.ShardIndex] = true
				}
			}
		}
	}

	data, err := enc.Join(shards, fileMeta.Size)
	if err != nil {
		return "", nil, rebuildResult, fmt.Errorf("join shards: %w", err)
	}

	return fileMeta.Name, data, rebuildResult, nil
}

func (s *Service) GetFileReader(fileID string) (string, io.ReadCloser, int64, *RebuildResult, error) {
	name, data, rebuildResult, err := s.DownloadFile(fileID)
	if err != nil {
		return "", nil, 0, rebuildResult, err
	}
	return name, io.NopCloser(bytes.NewReader(data)), int64(len(data)), rebuildResult, nil
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
