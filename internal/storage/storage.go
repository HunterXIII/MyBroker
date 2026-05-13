package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/HunterXIII/MyBroker/internal/metrics"
	"github.com/HunterXIII/MyBroker/internal/models"
)

type MsgToDLQ struct {
	Msg       models.Message `json:"msg"`
	FailedBy  string         `json:"failed_by"`
	Timestamp time.Time      `json:"timestamp"`
}

type StorageService struct {
	dir        string
	msgFile    *os.File
	msgLogPath string
	subsPath   string
	offsetPath string
	dlqPath    string

	mu            sync.RWMutex
	currentOffset uint64
	topicOffsets  map[string][]uint64
	offsets       map[string]uint64
	subscriptions map[string][]string
	dlq           []MsgToDLQ

	Log *slog.Logger
}

func NewStorageService(logger *slog.Logger, dir string) (*StorageService, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	s := &StorageService{
		dir:           dir,
		Log:           logger,
		msgLogPath:    filepath.Join(dir, "messages.json"),
		subsPath:      filepath.Join(dir, "subscriptions.json"),
		offsetPath:    filepath.Join(dir, "offsets.json"),
		dlqPath:       filepath.Join(dir, "dlq.json"),
		topicOffsets:  make(map[string][]uint64),
		offsets:       make(map[string]uint64),
		subscriptions: make(map[string][]string),
		dlq:           []MsgToDLQ{},
	}

	var err error
	s.msgFile, err = os.OpenFile(s.msgLogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	if err := s.loadState(); err != nil {
		return nil, err
	}

	return s, nil
}

// func (s *StorageService) loadState() error {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	if data, err := os.ReadFile(s.subsPath); err == nil {
// 		json.Unmarshal(data, &s.subscriptions)
// 	}

// 	if data, err := os.ReadFile(s.offsetPath); err == nil {
// 		json.Unmarshal(data, &s.offsets)
// 	}

// 	s.currentOffset = s.getLastOffsetFromLog()

// 	s.Log.Info("Storage state loaded",
// 		"last_offset", s.currentOffset,
// 		"active_sessions", len(s.offsets))

// 	return nil
// }

func (s *StorageService) loadState() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if data, err := os.ReadFile(s.subsPath); err == nil {
		json.Unmarshal(data, &s.subscriptions)
	}

	if data, err := os.ReadFile(s.offsetPath); err == nil {
		json.Unmarshal(data, &s.offsets)
	}

	messages, err := s.getAllMessages()
	if err != nil {
		if os.IsNotExist(err) {
			s.Log.Info("No message log found, starting with empty index")
			return nil
		}
		return fmt.Errorf("failed to read messages for indexing: %w", err)
	}

	s.topicOffsets = make(map[string][]uint64)
	maxOffset := uint64(0)

	for _, msg := range messages {
		s.topicOffsets[msg.Topic] = append(s.topicOffsets[msg.Topic], msg.Offset)

		if msg.Offset > maxOffset {
			maxOffset = msg.Offset
		}
	}

	s.currentOffset = maxOffset

	s.Log.Info("Storage state loaded",
		"last_offset", s.currentOffset,
		"active_sessions", len(s.offsets),
		"indexed_topics", len(s.topicOffsets),
		"total_messages_indexed", len(messages))

	return nil
}

func (s *StorageService) saveGlobalCheckpoint() {
	offset := s.currentOffset

	checkpointPath := filepath.Join(s.dir, "global_checkpoint.txt")
	_ = os.WriteFile(checkpointPath, []byte(strconv.FormatUint(offset, 10)), 0644)
}

func (s *StorageService) getLastOffsetFromLog() uint64 {
	var maxOffset uint64

	checkpointPath := filepath.Join(s.dir, "global_checkpoint.txt")
	if data, err := os.ReadFile(checkpointPath); err == nil {
		if val, err := strconv.ParseUint(string(data), 10, 64); err == nil {
			maxOffset = val
		}
	}

	file, err := os.Open(s.msgLogPath)
	if err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var msg models.Message
			if err := json.Unmarshal(scanner.Bytes(), &msg); err == nil {
				if msg.Offset > maxOffset {
					maxOffset = msg.Offset
				}
			}
		}
	}

	s.currentOffset = maxOffset

	go s.startCheckpointWorker()

	return maxOffset
}

func (s *StorageService) startCheckpointWorker() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.flushOffsetsToDisk()
	}
}

func (s *StorageService) flushOffsetsToDisk() {
	s.mu.RLock()
	data, err := json.MarshalIndent(s.offsets, "", "  ")
	s.mu.RUnlock()

	if err != nil {
		s.Log.Error("Failed to marshal offsets for checkpoint", "err", err)
		return
	}

	tmpPath := s.offsetPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		s.Log.Error("Failed to write offsets checkpoint", "err", err)
		return
	}

	if err := os.Rename(tmpPath, s.offsetPath); err != nil {
		s.Log.Error("Failed to commit offsets checkpoint", "err", err)
	}
}

func (s *StorageService) GetMessagesSince(offset uint64) ([]models.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	file, err := os.Open(s.msgLogPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var result []models.Message
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var msg models.Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err == nil {
			if msg.Offset > offset {
				if time.Now().Before(msg.ExpiresAt) {
					result = append(result, msg)
				}
			}
		}
	}
	s.Log.Debug("Get messages", "offset", offset, "count", len(result))
	sort.Slice(result, func(i, j int) bool {
		return result[i].Offset < result[j].Offset
	})
	return result, nil
}

func (s *StorageService) getAllMessages() ([]models.Message, error) {
	// s.mu.RLock()
	// defer s.mu.RUnlock()

	file, err := os.Open(s.msgLogPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var result []models.Message
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var msg models.Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err == nil {
			result = append(result, msg)
		}
	}
	s.Log.Debug("Get all messages", "count", len(result))
	return result, nil
}

func (s *StorageService) SaveMessage(msg *models.Message) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentOffset++
	msg.Offset = s.currentOffset

	data, err := json.Marshal(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal message: %w", err)
	}

	if _, err := s.msgFile.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write to log: %w", err)
	}

	if _, err := s.msgFile.WriteString("\n"); err != nil {
		return 0, fmt.Errorf("failed to write newline: %w", err)
	}

	if err := s.msgFile.Sync(); err != nil {
		s.Log.Error("Failed to sync log file", "err", err)
	}

	s.Log.Debug("Message saved", "offset", s.currentOffset, "topic", msg.Topic)

	s.saveGlobalCheckpoint()
	s.topicOffsets[msg.Topic] = append(s.topicOffsets[msg.Topic], msg.Offset)
	return s.currentOffset, nil
}

func (s *StorageService) GetCurrentFileSize() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info, err := s.msgFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file stat: %w", err)
	}
	s.Log.Debug("Get info of the log file", "info", info)

	return info.Size(), nil
}

func (s *StorageService) SaveSubscription(clientID, topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	exists := false
	for _, t := range s.subscriptions[clientID] {
		if t == topic {
			exists = true
			break
		}
	}

	if !exists {
		s.subscriptions[clientID] = append(s.subscriptions[clientID], topic)
	}

	data, err := json.MarshalIndent(s.subscriptions, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal subscriptions: %w", err)
	}

	err = os.WriteFile(s.subsPath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to save subscriptions file: %w", err)
	}

	s.Log.Debug("Subscription saved to disk", "client", clientID, "topic", topic)
	return nil
}

func (s *StorageService) RemoveSubscription(clientID, topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for idx, t := range s.subscriptions[clientID] {
		if t == topic {
			s.subscriptions[clientID] = append(s.subscriptions[clientID][:idx], s.subscriptions[clientID][idx+1:]...)
			return
		}
	}

}

func (s *StorageService) GetClientOffset(clientID string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clientOff, ok := s.offsets[clientID]
	if !ok {
		return s.currentOffset
	}

	return clientOff
}

func (s *StorageService) MarkAsDelivered(clientID string, offset uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, exists := s.offsets[clientID]

	if !exists || offset > current {
		s.offsets[clientID] = offset
		s.Log.Debug("Client progress updated in memory",
			"ClientID", clientID,
			"NewOffset", offset)
	}
}

func (s *StorageService) MoveToDLQ(clientID string, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg, err := s.getMessageByOffset(offset)
	if err != nil {
		return fmt.Errorf("failed to retrieve message for DLQ: %w", err)
	}

	dlqEntry := MsgToDLQ{
		Msg:       msg,
		FailedBy:  clientID,
		Timestamp: time.Now(),
	}
	s.dlq = append(s.dlq, dlqEntry)

	data, err := json.MarshalIndent(s.dlq, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ: %w", err)
	}

	err = os.WriteFile(s.dlqPath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to save DLQ file: %w", err)
	}

	s.Log.Info("Message moved to DLQ", "ClientID", clientID, "Topic", msg.Topic, "Offset", msg.Offset)
	metrics.MsgInDLQ.Inc()
	return nil
}

func (s *StorageService) getMessageByOffset(offset uint64) (models.Message, error) {
	file, err := os.Open(s.msgLogPath)
	if err != nil {
		return models.Message{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var msg models.Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err == nil {
			if msg.Offset == offset {
				return msg, nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return models.Message{}, fmt.Errorf("error scanning log file: %w", err)
	}

	return models.Message{}, fmt.Errorf("message with offset %d not found", offset)
}

func (s *StorageService) сleanExpiredMessages() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Log.Info("GC: Cleanup started")

	now := time.Now()
	expiredCount := 0
	validCount := 0

	messages, err := s.getAllMessages()
	if err != nil {
		return fmt.Errorf("failed to get all messages: %w", err)
	}

	tempFileName := filepath.Join(s.dir, "messages.json.tmp")
	tempFile, err := os.OpenFile(tempFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	newTopicOffsets := make(map[string][]uint64)

	for _, msg := range messages {
		if msg.ExpiresAt.After(now) {
			data, err := json.Marshal(msg)
			if err != nil {
				tempFile.Close()
				return fmt.Errorf("failed to marshal message during cleanup: %w", err)
			}

			if _, err := tempFile.Write(data); err != nil {
				tempFile.Close()
				return fmt.Errorf("failed to write to temp file: %w", err)
			}
			if _, err := tempFile.WriteString("\n"); err != nil {
				tempFile.Close()
				return fmt.Errorf("failed to write newline to temp file: %w", err)
			}
			newTopicOffsets[msg.Topic] = append(newTopicOffsets[msg.Topic], msg.Offset)
			validCount++
		} else {
			expiredCount++
		}
	}

	tempFile.Sync()
	tempFile.Close()

	if expiredCount == 0 {
		s.Log.Info("GC: No expired messages found, cleaning up temp file")
		os.Remove(tempFileName)
		return nil
	}

	s.topicOffsets = newTopicOffsets

	if err := s.msgFile.Close(); err != nil {
		return fmt.Errorf("failed to close current msgFile: %w", err)
	}

	s.Log.Debug("GC: Debug paths", "temp", tempFileName, "target", s.msgLogPath)

	if err := os.Rename(tempFileName, s.msgLogPath); err != nil {
		s.msgFile, _ = os.OpenFile(s.msgLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	newFile, err := os.OpenFile(s.msgLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen messages file: %w", err)
	}
	s.msgFile = newFile

	s.Log.Info("GC: Cleanup finished", "removed", expiredCount, "remaining", validCount)
	s.saveGlobalCheckpoint()
	return nil
}

func (s *StorageService) StartGC(ctx context.Context, interval time.Duration) {
	s.Log.Info("GC: Background worker started", "interval", interval)

	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.сleanExpiredMessages(); err != nil {
					s.Log.Error("GC: Cleanup failed", "error", err)
				}
			case <-ctx.Done():
				s.Log.Info("GC: Background worker stopped")
				return
			}
		}
	}()
}

func (s *StorageService) GetConsumerLag(clientID string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lastConfirmed, ok := s.offsets[clientID]
	if !ok {
		lastConfirmed = 0
	}

	clientTopics := s.subscriptions[clientID]
	var totalLag uint64

	for _, topic := range clientTopics {
		offsets, exists := s.topicOffsets[topic]
		if !exists {
			continue
		}

		idx := sort.Search(len(offsets), func(i int) bool {
			return offsets[i] > lastConfirmed
		})

		totalLag += uint64(len(offsets) - idx)
	}

	return totalLag
}

func (s *StorageService) GetTopicsByClient(clientID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if topics, ok := s.subscriptions[clientID]; ok {
		cp := make([]string, len(topics))
		copy(cp, topics)
		return cp
	}
	return nil
}
