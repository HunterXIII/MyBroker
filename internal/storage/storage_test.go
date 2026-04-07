package storage

import (
	"log/slog"
	"testing"
)

func TestNewStorageService_Isolated(t *testing.T) {
	tmp := t.TempDir() // Магия Go: сама создаст и сама удалит папку
	logger := slog.Default()

	svc, err := NewStorageService(logger, tmp)
	if err != nil {
		t.Fatal(err)
	}

	if svc.msgFile == nil || svc.offsetFile == nil {
		t.Fatal("Files should not be nil")
	}

	// Проверяем запись
	testData := []byte("hello broker")
	_, err = svc.msgFile.Write(testData)
	if err != nil {
		t.Errorf("Should be able to write to msgFile: %v", err)
	}
}
