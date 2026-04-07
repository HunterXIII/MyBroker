package storage

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/HunterXIII/MyBroker/internal/models"
)

type StorageService struct {
	msgFile    *os.File
	offsetFile *os.File
	mu         sync.RWMutex
	Log        *slog.Logger
}

func NewStorageService(logger *slog.Logger, dir string) (*StorageService, error) {
	msgPath := filepath.Join(dir, "messages.log")
	offsetPath := filepath.Join(dir, "processed.idx")

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	msgFile, err := os.OpenFile(msgPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	offsetFile, err := os.OpenFile(offsetPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	return &StorageService{
		Log:        logger,
		msgFile:    msgFile,
		offsetFile: offsetFile,
	}, nil
}

func (s *StorageService) SaveMessage(msg *models.Message) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msgStr := fmt.Sprintf("%d|%s|%s|%s\n", msg.Timestamp, msg.ID, msg.Topic, msg.Payload)

	_, err := s.msgFile.WriteString(msgStr)
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	s.msgFile.Sync()
	s.Log.Debug("Sync log file")

	size, err := s.GetCurrentFileSize()
	if err != nil {
		return err
	}
	s.Log.Debug("Changed log file", "size", size)
	return s.SaveOffset(size)
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

func (s *StorageService) SaveOffset(offset int64) error {

	if err := s.offsetFile.Truncate(0); err != nil {
		return err
	}

	_, err := s.offsetFile.WriteAt([]byte(fmt.Sprintf("%d", offset)), 0)
	return err
}

func (s *StorageService) LoadOffset() (int64, error) {

	_, err := s.offsetFile.Seek(0, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to seek offset file: %w", err)
	}

	data := make([]byte, 32)

	n, err := s.offsetFile.Read(data)
	if err != nil {
		if err.Error() == "EOF" {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to read offset: %w", err)
	}

	cleanData := string(data[:n])

	offset, err := strconv.ParseInt(cleanData, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse offset '%s': %w", cleanData, err)
	}

	return offset, nil
}

func (s *StorageService) LoadUnprocessed() ([]*models.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset, err := s.LoadOffset()
	if err != nil {
		return nil, err
	}

	_, err = s.msgFile.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(s.msgFile)
	var messages []*models.Message

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		msg, err := s.parseLogLine(line)
		if err != nil {
			s.Log.Error("failed to parse log line", "line", line, "err", err)
			continue
		}
		messages = append(messages, msg)
		s.Log.Debug("Recovered a new message", "MsgID", msg.ID, "Topic", msg.Topic, "service", "StorageService")
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	s.Log.Info("Unprocessed messages found", "Count", len(messages), "service", "StorageService")

	return messages, nil
}

func (s *StorageService) parseLogLine(line string) (*models.Message, error) {
	parts := strings.Split(line, "|")
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid log format: expected 4 parts, got %d", len(parts))
	}

	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}

	return &models.Message{
		Timestamp: ts,
		ID:        parts[1],
		Topic:     parts[2],
		Payload:   []byte(parts[3]),
	}, nil
}
