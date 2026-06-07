package model

import "time"

type File struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Size          int64     `json:"size"`
	OriginalHash  string    `json:"original_hash"`
	ShardSize     int64     `json:"shard_size"`
	DataShards    int       `json:"data_shards"`
	ParityShards  int       `json:"parity_shards"`
	TotalShards   int       `json:"total_shards"`
	CreatedAt     time.Time `json:"created_at"`
}

type Shard struct {
	ID        int64     `json:"id"`
	FileID    string    `json:"file_id"`
	ShardIndex int     `json:"shard_index"`
	NodeID    int       `json:"node_id"`
	Size      int64     `json:"size"`
	Hash      string    `json:"hash"`
	IsParity  bool      `json:"is_parity"`
	StoredAt  time.Time `json:"stored_at"`
}

type Node struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	Path      string    `json:"path"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type RebuildLog struct {
	ID              int64     `json:"id"`
	FileID          string    `json:"file_id"`
	FailedNodeIDs   string    `json:"failed_node_ids"`
	StartTime       time.Time `json:"start_time"`
	EndTime         time.Time `json:"end_time"`
	DurationMs      int64     `json:"duration_ms"`
	DataSize        int64     `json:"data_size"`
	HashVerified    bool      `json:"hash_verified"`
	RebuiltShards   string    `json:"rebuilt_shards"`
	Status          string    `json:"status"`
	ErrorMessage    string    `json:"error_message,omitempty"`
}
