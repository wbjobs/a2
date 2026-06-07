package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"rs-service/internal/model"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const (
	NodeStatusOnline    = "online"
	NodeStatusOffline   = "offline"
	RebuildStatusSuccess = "success"
	RebuildStatusFailed  = "failed"

	maxRetries      = 5
	retryInterval   = 50 * time.Millisecond
	busyTimeoutMs   = 5000
)

var db *sql.DB

func Init(dbPath string) error {
	var err error
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create db dir: %w", err)
	}

	dsn := fmt.Sprintf("%s?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(%d)&_pragma=synchronous(NORMAL)",
		dbPath, busyTimeoutMs)

	db, err = sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("enable WAL mode: %w", err)
	}

	if _, err := db.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeoutMs)); err != nil {
		return fmt.Errorf("set busy timeout: %w", err)
	}

	if err := createTables(); err != nil {
		return fmt.Errorf("create tables: %w", err)
	}

	if err := initNodes(); err != nil {
		return fmt.Errorf("init nodes: %w", err)
	}

	return nil
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "database is locked") ||
		strings.Contains(errStr, "SQLITE_BUSY") ||
		strings.Contains(errStr, "SQLITE_LOCKED")
}

func execWithRetry(query string, args ...interface{}) (sql.Result, error) {
	var result sql.Result
	var err error

	for i := 0; i < maxRetries; i++ {
		result, err = db.Exec(query, args...)
		if err == nil {
			return result, nil
		}
		if !isRetryableError(err) {
			return nil, err
		}
		if i < maxRetries-1 {
			time.Sleep(retryInterval * time.Duration(i+1))
		}
	}
	return nil, fmt.Errorf("exec failed after %d retries: %w", maxRetries, err)
}

func queryWithRetry(query string, args ...interface{}) (*sql.Rows, error) {
	var rows *sql.Rows
	var err error

	for i := 0; i < maxRetries; i++ {
		rows, err = db.Query(query, args...)
		if err == nil {
			return rows, nil
		}
		if !isRetryableError(err) {
			return nil, err
		}
		if i < maxRetries-1 {
			time.Sleep(retryInterval * time.Duration(i+1))
		}
	}
	return nil, fmt.Errorf("query failed after %d retries: %w", maxRetries, err)
}

func queryRowWithRetry(query string, args ...interface{}) *sql.Row {
	var row *sql.Row
	var err error

	for i := 0; i < maxRetries; i++ {
		row = db.QueryRow(query, args...)
		err = row.Err()
		if err == nil {
			return row
		}
		if !isRetryableError(err) {
			return row
		}
		if i < maxRetries-1 {
			time.Sleep(retryInterval * time.Duration(i+1))
		}
	}
	return row
}

func createTables() error {
	sqlStmt := `
	CREATE TABLE IF NOT EXISTS files (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		size INTEGER NOT NULL,
		original_hash TEXT NOT NULL,
		shard_size INTEGER NOT NULL,
		data_shards INTEGER NOT NULL,
		parity_shards INTEGER NOT NULL,
		total_shards INTEGER NOT NULL,
		created_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS shards (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		file_id TEXT NOT NULL,
		shard_index INTEGER NOT NULL,
		node_id INTEGER NOT NULL,
		size INTEGER NOT NULL,
		hash TEXT NOT NULL,
		is_parity INTEGER NOT NULL,
		stored_at DATETIME NOT NULL,
		FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
		UNIQUE(file_id, shard_index)
	);

	CREATE TABLE IF NOT EXISTS nodes (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		path TEXT NOT NULL,
		status TEXT NOT NULL,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS rebuild_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		file_id TEXT NOT NULL,
		failed_node_ids TEXT NOT NULL,
		start_time DATETIME NOT NULL,
		end_time DATETIME NOT NULL,
		duration_ms INTEGER NOT NULL,
		data_size INTEGER NOT NULL,
		hash_verified INTEGER NOT NULL,
		rebuilt_shards TEXT NOT NULL,
		status TEXT NOT NULL,
		error_message TEXT,
		FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
	);

	CREATE INDEX IF NOT EXISTS idx_shards_file_id ON shards(file_id);
	CREATE INDEX IF NOT EXISTS idx_rebuild_logs_file_id ON rebuild_logs(file_id);
	CREATE INDEX IF NOT EXISTS idx_shards_node_id ON shards(node_id);
	`

	_, err := execWithRetry(sqlStmt)
	return err
}

func initNodes() error {
	var count int
	err := queryRowWithRetry("SELECT COUNT(*) FROM nodes").Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	now := time.Now()
	for i := 0; i < 9; i++ {
		nodePath := filepath.Join("nodes", fmt.Sprintf("node_%d", i))
		absPath, err := filepath.Abs(nodePath)
		if err != nil {
			absPath = nodePath
		}
		_, err = execWithRetry(`
			INSERT INTO nodes (id, name, path, status, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?)
		`, i, fmt.Sprintf("node_%d", i), absPath, NodeStatusOnline, now, now)
		if err != nil {
			return err
		}
	}
	return nil
}

func Close() error {
	if db != nil {
		return db.Close()
	}
	return nil
}

func GetDB() *sql.DB {
	return db
}

func InsertFile(f *model.File) error {
	_, err := execWithRetry(`
		INSERT INTO files (id, name, size, original_hash, shard_size, data_shards, parity_shards, total_shards, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, f.ID, f.Name, f.Size, f.OriginalHash, f.ShardSize, f.DataShards, f.ParityShards, f.TotalShards, f.CreatedAt)
	return err
}

func GetFile(fileID string) (*model.File, error) {
	var f model.File
	err := queryRowWithRetry(`
		SELECT id, name, size, original_hash, shard_size, data_shards, parity_shards, total_shards, created_at
		FROM files WHERE id = ?
	`, fileID).Scan(&f.ID, &f.Name, &f.Size, &f.OriginalHash, &f.ShardSize, &f.DataShards, &f.ParityShards, &f.TotalShards, &f.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &f, nil
}

func ListFiles() ([]*model.File, error) {
	rows, err := queryWithRetry(`
		SELECT id, name, size, original_hash, shard_size, data_shards, parity_shards, total_shards, created_at
		FROM files ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []*model.File
	for rows.Next() {
		var f model.File
		err := rows.Scan(&f.ID, &f.Name, &f.Size, &f.OriginalHash, &f.ShardSize, &f.DataShards, &f.ParityShards, &f.TotalShards, &f.CreatedAt)
		if err != nil {
			return nil, err
		}
		files = append(files, &f)
	}
	return files, rows.Err()
}

func InsertShard(s *model.Shard) error {
	_, err := execWithRetry(`
		INSERT INTO shards (file_id, shard_index, node_id, size, hash, is_parity, stored_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, s.FileID, s.ShardIndex, s.NodeID, s.Size, s.Hash, s.IsParity, s.StoredAt)
	return err
}

func GetShardsByFile(fileID string) ([]*model.Shard, error) {
	rows, err := queryWithRetry(`
		SELECT id, file_id, shard_index, node_id, size, hash, is_parity, stored_at
		FROM shards WHERE file_id = ? ORDER BY shard_index
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var shards []*model.Shard
	for rows.Next() {
		var s model.Shard
		var isParity int
		err := rows.Scan(&s.ID, &s.FileID, &s.ShardIndex, &s.NodeID, &s.Size, &s.Hash, &isParity, &s.StoredAt)
		if err != nil {
			return nil, err
		}
		s.IsParity = isParity == 1
		shards = append(shards, &s)
	}
	return shards, rows.Err()
}

func GetNodes() ([]*model.Node, error) {
	rows, err := queryWithRetry(`
		SELECT id, name, path, status, created_at, updated_at
		FROM nodes ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []*model.Node
	for rows.Next() {
		var n model.Node
		err := rows.Scan(&n.ID, &n.Name, &n.Path, &n.Status, &n.CreatedAt, &n.UpdatedAt)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, &n)
	}
	return nodes, rows.Err()
}

func GetNode(nodeID int) (*model.Node, error) {
	var n model.Node
	err := queryRowWithRetry(`
		SELECT id, name, path, status, created_at, updated_at
		FROM nodes WHERE id = ?
	`, nodeID).Scan(&n.ID, &n.Name, &n.Path, &n.Status, &n.CreatedAt, &n.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func SetNodeStatus(nodeID int, status string) error {
	_, err := execWithRetry(`
		UPDATE nodes SET status = ?, updated_at = ? WHERE id = ?
	`, status, time.Now(), nodeID)
	return err
}

func GetOnlineNodeIDs() ([]int, error) {
	rows, err := queryWithRetry("SELECT id FROM nodes WHERE status = ? ORDER BY id", NodeStatusOnline)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func InsertRebuildLog(log *model.RebuildLog) error {
	var hashVerified int
	if log.HashVerified {
		hashVerified = 1
	}
	_, err := execWithRetry(`
		INSERT INTO rebuild_logs (file_id, failed_node_ids, start_time, end_time, duration_ms, 
			data_size, hash_verified, rebuilt_shards, status, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, log.FileID, log.FailedNodeIDs, log.StartTime, log.EndTime, log.DurationMs,
		log.DataSize, hashVerified, log.RebuiltShards, log.Status, log.ErrorMessage)
	return err
}

func ListRebuildLogs(fileID string) ([]*model.RebuildLog, error) {
	var rows *sql.Rows
	var err error

	if fileID != "" {
		rows, err = queryWithRetry(`
			SELECT id, file_id, failed_node_ids, start_time, end_time, duration_ms,
				data_size, hash_verified, rebuilt_shards, status, COALESCE(error_message, '')
			FROM rebuild_logs WHERE file_id = ? ORDER BY start_time DESC
		`, fileID)
	} else {
		rows, err = queryWithRetry(`
			SELECT id, file_id, failed_node_ids, start_time, end_time, duration_ms,
				data_size, hash_verified, rebuilt_shards, status, COALESCE(error_message, '')
			FROM rebuild_logs ORDER BY start_time DESC
		`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []*model.RebuildLog
	for rows.Next() {
		var l model.RebuildLog
		var hashVerified int
		err := rows.Scan(&l.ID, &l.FileID, &l.FailedNodeIDs, &l.StartTime, &l.EndTime, &l.DurationMs,
			&l.DataSize, &hashVerified, &l.RebuiltShards, &l.Status, &l.ErrorMessage)
		if err != nil {
			return nil, err
		}
		l.HashVerified = hashVerified == 1
		logs = append(logs, &l)
	}
	return logs, rows.Err()
}

func UpdateShardNode(fileID string, shardIndex int, newNodeID int) error {
	_, err := execWithRetry(`
		UPDATE shards SET node_id = ? WHERE file_id = ? AND shard_index = ?
	`, newNodeID, fileID, shardIndex)
	return err
}

func UpdateShardHashAndSize(fileID string, shardIndex int, hash string, size int64) error {
	_, err := execWithRetry(`
		UPDATE shards SET hash = ?, size = ?, stored_at = ? WHERE file_id = ? AND shard_index = ?
	`, hash, size, time.Now(), fileID, shardIndex)
	return err
}

func GetFilesOnNode(nodeID int) ([]string, error) {
	rows, err := queryWithRetry(`
		SELECT DISTINCT file_id FROM shards WHERE node_id = ?
	`, nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fileIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		fileIDs = append(fileIDs, id)
	}
	return fileIDs, rows.Err()
}

func IntsToString(ids []int) string {
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(strs, ",")
}
