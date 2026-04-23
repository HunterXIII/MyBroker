package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/HunterXIII/MyBroker/internal/models"
)

type StorageService struct {
	msgFile    *os.File
	msgLogPath string
	subsPath   string
	offsetPath string

	mu            sync.RWMutex
	currentOffset uint64
	offsets       map[string]uint64
	subscriptions map[string][]string

	Log *slog.Logger
}

func NewStorageService(logger *slog.Logger, dir string) (*StorageService, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	s := &StorageService{
		Log:           logger,
		msgLogPath:    filepath.Join(dir, "messages.json"),
		subsPath:      filepath.Join(dir, "subscriptions.json"),
		offsetPath:    filepath.Join(dir, "offsets.json"),
		offsets:       make(map[string]uint64),
		subscriptions: make(map[string][]string),
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

func (s *StorageService) loadState() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if data, err := os.ReadFile(s.subsPath); err == nil {
		json.Unmarshal(data, &s.subscriptions)
	}

	if data, err := os.ReadFile(s.offsetPath); err == nil {
		json.Unmarshal(data, &s.offsets)
	}

	s.currentOffset = s.getLastOffsetFromLog()

	s.Log.Info("Storage state loaded",
		"last_offset", s.currentOffset,
		"active_sessions", len(s.offsets))

	return nil
}

func (s *StorageService) getLastOffsetFromLog() uint64 {
	file, err := os.Open(s.msgLogPath)
	if err != nil {
		return 0
	}
	defer file.Close()

	var lastOffset uint64
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var msg models.Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err == nil {
			lastOffset = msg.Offset
		}
	}

	if err := scanner.Err(); err != nil {
		s.Log.Error("Error scanning log file", "err", err)
		return 0
	}

	go s.startCheckpointWorker()

	return lastOffset
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

	if offset, ok := s.offsets[clientID]; ok {
		return offset
	}

	return s.currentOffset
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
