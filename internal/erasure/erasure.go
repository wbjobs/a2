package erasure

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/klauspost/reedsolomon"
)

const (
	DataShards   = 6
	ParityShards = 3
	TotalShards  = DataShards + ParityShards
)

type Encoder struct {
	enc reedsolomon.Encoder
}

func NewEncoder() (*Encoder, error) {
	enc, err := reedsolomon.New(DataShards, ParityShards)
	if err != nil {
		return nil, fmt.Errorf("create reedsolomon encoder: %w", err)
	}
	return &Encoder{enc: enc}, nil
}

func (e *Encoder) Encode(data []byte) ([][]byte, int64, error) {
	dataLen := int64(len(data))
	if dataLen == 0 {
		return nil, 0, fmt.Errorf("empty data")
	}

	shardSize := (dataLen + int64(DataShards) - 1) / int64(DataShards)
	paddedLen := shardSize * int64(DataShards)

	paddedData := make([]byte, paddedLen)
	copy(paddedData, data)

	shards := make([][]byte, TotalShards)
	for i := 0; i < DataShards; i++ {
		start := int64(i) * shardSize
		end := start + shardSize
		shards[i] = paddedData[start:end]
	}

	for i := DataShards; i < TotalShards; i++ {
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

func (e *Encoder) Reconstruct(shards [][]byte, dataShardsPresent []bool) error {
	if len(shards) != TotalShards {
		return fmt.Errorf("expected %d shards, got %d", TotalShards, len(shards))
	}

	if len(dataShardsPresent) != TotalShards {
		return fmt.Errorf("expected %d present flags, got %d", TotalShards, len(dataShardsPresent))
	}

	missingCount := 0
	for i, present := range dataShardsPresent {
		if !present {
			shards[i] = nil
			missingCount++
		}
	}

	if missingCount > ParityShards {
		return fmt.Errorf("too many missing shards: %d, max recoverable: %d", missingCount, ParityShards)
	}

	if err := e.enc.Reconstruct(shards); err != nil {
		return fmt.Errorf("reconstruct shards: %w", err)
	}

	return nil
}

func (e *Encoder) ReconstructData(shards [][]byte, dataShardsPresent []bool) error {
	if len(shards) != TotalShards {
		return fmt.Errorf("expected %d shards, got %d", TotalShards, len(shards))
	}

	if len(dataShardsPresent) != TotalShards {
		return fmt.Errorf("expected %d present flags, got %d", TotalShards, len(dataShardsPresent))
	}

	for i, present := range dataShardsPresent {
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
	var buf bytes.Buffer
	for i := 0; i < DataShards; i++ {
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
