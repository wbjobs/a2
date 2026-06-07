package erasure

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
)

const (
	SmallFileThreshold = 1 * 1024 * 1024
	LargeFileThreshold = 10 * 1024 * 1024

	DefaultDataShards   = 6
	DefaultParityShards = 3
	DefaultTotalShards  = DefaultDataShards + DefaultParityShards

	MaxTotalShards = 14
	MaxDataShards  = 10
	MaxParityShards = 4
)

type CodecConfig struct {
	DataShards   int
	ParityShards int
	TotalShards  int
	Name         string
}

var (
	ConfigRS42  = CodecConfig{DataShards: 4, ParityShards: 2, TotalShards: 6, Name: "RS(4,2)"}
	ConfigRS63  = CodecConfig{DataShards: 6, ParityShards: 3, TotalShards: 9, Name: "RS(6,3)"}
	ConfigRS104 = CodecConfig{DataShards: 10, ParityShards: 4, TotalShards: 14, Name: "RS(10,4)"}
)

func SelectConfig(fileSize int64) CodecConfig {
	if fileSize < SmallFileThreshold {
		return ConfigRS42
	} else if fileSize < LargeFileThreshold {
		return ConfigRS63
	}
	return ConfigRS104
}

type Encoder struct {
	enc    reedsolomon.Encoder
	config CodecConfig
	mu     sync.Mutex
}

func NewEncoder(config CodecConfig) (*Encoder, error) {
	enc, err := reedsolomon.New(config.DataShards, config.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("create reedsolomon encoder for %s: %w", config.Name, err)
	}
	return &Encoder{
		enc:    enc,
		config: config,
	}, nil
}

func NewDefaultEncoder() (*Encoder, error) {
	return NewEncoder(ConfigRS63)
}

func (e *Encoder) Config() CodecConfig {
	return e.config
}

func (e *Encoder) Encode(data []byte) ([][]byte, int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	dataLen := int64(len(data))
	if dataLen == 0 {
		return nil, 0, fmt.Errorf("empty data")
	}

	shardSize := (dataLen + int64(e.config.DataShards) - 1) / int64(e.config.DataShards)
	paddedLen := shardSize * int64(e.config.DataShards)

	paddedData := make([]byte, paddedLen)
	copy(paddedData, data)

	shards := make([][]byte, e.config.TotalShards)
	for i := 0; i < e.config.DataShards; i++ {
		start := int64(i) * shardSize
		end := start + shardSize
		shards[i] = paddedData[start:end]
	}

	for i := e.config.DataShards; i < e.config.TotalShards; i++ {
		shards[i] = make([]byte, shardSize)
	}

	if err := e.enc.Encode(shards); err != nil {
		return nil, 0, fmt.Errorf("encode shards: %w", err)
	}

	return shards, shardSize, nil
}

func (e *Encoder) EncodeReader(r io.Reader, dataLen int64) ([][]byte, int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, 0, fmt.Errorf("read data: %w", err)
	}
	if int64(len(data)) != dataLen {
		return nil, 0, fmt.Errorf("read %d bytes, expected %d", len(data), dataLen)
	}
	return e.Encode(data)
}

func (e *Encoder) Reconstruct(shards [][]byte, shardsPresent []bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(shards) != e.config.TotalShards {
		return fmt.Errorf("expected %d shards, got %d", e.config.TotalShards, len(shards))
	}

	if len(shardsPresent) != e.config.TotalShards {
		return fmt.Errorf("expected %d present flags, got %d", e.config.TotalShards, len(shardsPresent))
	}

	missingCount := 0
	for i, present := range shardsPresent {
		if !present {
			shards[i] = nil
			missingCount++
		}
	}

	if missingCount > e.config.ParityShards {
		return fmt.Errorf("too many missing shards: %d, max recoverable: %d", missingCount, e.config.ParityShards)
	}

	if err := e.enc.Reconstruct(shards); err != nil {
		return fmt.Errorf("reconstruct shards: %w", err)
	}

	return nil
}

func (e *Encoder) ReconstructData(shards [][]byte, shardsPresent []bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(shards) != e.config.TotalShards {
		return fmt.Errorf("expected %d shards, got %d", e.config.TotalShards, len(shards))
	}

	if len(shardsPresent) != e.config.TotalShards {
		return fmt.Errorf("expected %d present flags, got %d", e.config.TotalShards, len(shardsPresent))
	}

	for i, present := range shardsPresent {
		if !present {
			shards[i] = nil
		}
	}

	if err := e.enc.ReconstructData(shards); err != nil {
		return fmt.Errorf("reconstruct data shards: %w", err)
	}

	return nil
}

func (e *Encoder) Join(shards [][]byte, originalSize int64) ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var buf bytes.Buffer
	for i := 0; i < e.config.DataShards; i++ {
		if shards[i] == nil {
			return nil, fmt.Errorf("data shard %d is nil", i)
		}
		buf.Write(shards[i])
	}

	data := buf.Bytes()
	if int64(len(data)) < originalSize {
		return nil, fmt.Errorf("joined data too short: %d bytes, expected %d", len(data), originalSize)
	}

	return data[:originalSize], nil
}

func (e *Encoder) Verify(shards [][]byte) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.enc.Verify(shards)
}

func HashData(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func HashReader(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func SplitData(data []byte, chunkSize int64) [][]byte {
	var chunks [][]byte
	dataLen := int64(len(data))
	for i := int64(0); i < dataLen; i += chunkSize {
		end := i + chunkSize
		if end > dataLen {
			end = dataLen
		}
		chunks = append(chunks, data[i:end])
	}
	return chunks
}

func GetConfig(dataShards, parityShards int) (CodecConfig, error) {
	switch {
	case dataShards == 4 && parityShards == 2:
		return ConfigRS42, nil
	case dataShards == 6 && parityShards == 3:
		return ConfigRS63, nil
	case dataShards == 10 && parityShards == 4:
		return ConfigRS104, nil
	default:
		return CodecConfig{}, fmt.Errorf("unsupported RS configuration: data=%d, parity=%d", dataShards, parityShards)
	}
}

type EncoderPool struct {
	pool map[string]*Encoder
	mu   sync.Mutex
}

var encoderPool = &EncoderPool{
	pool: make(map[string]*Encoder),
}

func GetEncoder(dataShards, parityShards int) (*Encoder, error) {
	key := fmt.Sprintf("%d_%d", dataShards, parityShards)

	encoderPool.mu.Lock()
	defer encoderPool.mu.Unlock()

	if enc, ok := encoderPool.pool[key]; ok {
		return enc, nil
	}

	config, err := GetConfig(dataShards, parityShards)
	if err != nil {
		return nil, err
	}

	enc, err := NewEncoder(config)
	if err != nil {
		return nil, err
	}

	encoderPool.pool[key] = enc
	return enc, nil
}
