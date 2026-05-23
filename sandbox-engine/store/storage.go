package store

import (
	"fmt"
	"mime/multipart"
	"os"
	"path/filepath"
	"sandbox-engine/model"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const uploadsDir = "./uploads"

var (
	mu          sync.RWMutex
	submissions = make(map[string]model.Submission)
)

func InitStorage() error {
	if err := os.MkdirAll(uploadsDir, 0755); err != nil {
		return fmt.Errorf("could not create uploads dir: %w", err)
	}
	return nil
}

func IsValidArchive(filename string) bool {
	lower := strings.ToLower(filename)
	return strings.HasSuffix(lower, ".zip") || strings.HasSuffix(lower, ".tar.gz")
}

func SaveSubmission(fileHeader *multipart.FileHeader, teamName, language string) (model.Submission, error) {

	id := uuid.New().String()

	safeFilename := fmt.Sprintf("%s_%s", id, filepath.Base(fileHeader.Filename))
	destPath := filepath.Join(uploadsDir, safeFilename)

	src, err := fileHeader.Open()
	if err != nil {
		return model.Submission{}, fmt.Errorf("cannot open uploaded file: %w", err)
	}
	defer src.Close()

	dst, err := os.Create(destPath)
	if err != nil {
		return model.Submission{}, fmt.Errorf("cannot create destination file: %w", err)
	}
	defer dst.Close()

	buf := make([]byte, 32*1024)
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
				return model.Submission{}, fmt.Errorf("write error: %w", writeErr)
			}
		}
		if readErr != nil {
			break
		}
	}

	submission := model.Submission{
		ID:          id,
		TeamName:    teamName,
		Language:    language,
		Filename:    fileHeader.Filename,
		StoragePath: destPath,
		Status:      model.StatusReceived,
		SubmittedAt: time.Now(),
	}

	mu.Lock()
	submissions[id] = submission
	mu.Unlock()

	return submission, nil
}

func GetAllSubmissions() []model.Submission {
	mu.RLock()
	defer mu.RUnlock()

	result := make([]model.Submission, 0, len(submissions))
	for _, s := range submissions {
		result = append(result, s)
	}
	return result
}

func GetSubmission(id string) (model.Submission, bool) {
	mu.RLock()
	defer mu.RUnlock()

	s, found := submissions[id]
	return s, found
}

func UpdateStatus(id string, status model.Status) error {
	mu.Lock()
	defer mu.Unlock()

	s, found := submissions[id]
	if !found {
		return fmt.Errorf("submission %s not found", id)
	}
	s.Status = status
	submissions[id] = s
	return nil
}
