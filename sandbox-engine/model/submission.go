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
