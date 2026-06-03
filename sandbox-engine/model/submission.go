package model

import "time"

type Status string

const (
	StatusReceived  Status = "RECEIVED"
	StatusCompiling Status = "COMPILING"
	StatusRunning   Status = "RUNNING"
	StatusFailed    Status = "FAILED"
	StatusCompleted Status = "COMPLETED"
)

type Submission struct {
	ID          string    `json:"id"`
	TeamName    string    `json:"team_name"`
	Language    string    `json:"language"`
	Filename    string    `json:"filename"`
	StoragePath string    `json:"storage_path"`
	Status      Status    `json:"status"`
	SubmittedAt time.Time `json:"submitted_at"`
	ContainerID string    `json:"container_id,omitempty"`
	EndpointURL string    `json:"endpoint_url,omitempty"`
}

type SubmissionReadyEvent struct {
	SubmissionID  string `json:"submission_id"`
	ContestantID  string `json:"contestant_id"`
	EndpointURL   string `json:"endpoint_url"`
	Port          int    `json:"port"`
	Language      string `json:"language"`
	SubmittedAt   string `json:"submitted_at"` // ISO8601
	CPULimit      int    `json:"cpu_limit"`
	MemoryLimit   int    `json:"memory_limit_mb"`
	Status        string `json:"status"`
}

