package api

import (
	"context"
	"log"
	"net/http"
	"os"
	"sandbox-engine/docker"
	"sandbox-engine/model"
	"sandbox-engine/publisher"
	"sandbox-engine/store"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

var runner *docker.Runner

func InitRunner(r *docker.Runner) {
	runner = r
}

func RegisterRoutes(router *gin.Engine) {
	router.POST("/submit", handleSubmit)
	router.GET("/submissions", handleListSubmissions)
	router.GET("/submissions/:id", handleGetSubmission)
	router.DELETE("/submissions/:id", handleStopSubmission)
	router.DELETE("/sandbox/:containerID", handleContainerDeletion)
}

func handleSubmit(c *gin.Context) {
	teamName := c.PostForm("team_name")
	language := c.PostForm("language")

	if teamName == "" || language == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "team_name and language fields are required",
		})
		return
	}

	validLanguages := map[string]bool{"cpp": true, "rust": true, "go": true}
	if !validLanguages[language] {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "language must be one of: cpp, rust, go",
		})
		return
	}

	fileHeader, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "No file uploaded. Use form field name 'file'",
		})
		return
	}

	if !store.IsValidArchive(fileHeader.Filename) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "File must be a .zip or .tar.gz archive",
		})
		return
	}

	submission, err := store.SaveSubmission(fileHeader, teamName, language)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to save submission: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":    "Submission received. Deploying in background...",
		"submission": submission,
	})

	go func() {
		log.Printf("[%s] Starting deployment for team: %s", submission.ID, submission.TeamName)
		store.UpdateStatus(submission.ID, model.StatusCompiling)

		info, err := runner.DeploySubmission(
			submission.ID,
			submission.StoragePath,
			submission.Language,
		)
		if err != nil {
			log.Printf("[%s] Deployment failed: %v", submission.ID, err)
			store.UpdateStatus(submission.ID, model.StatusFailed)
			store.UpdateEndpoint(submission.ID, "", "")
			return
		}

		store.UpdateStatus(submission.ID, model.StatusRunning)
		store.UpdateEndpoint(submission.ID, info.ContainerID, info.EndpointURL)
		log.Printf("[%s] ✅ Live at %s", submission.ID, info.EndpointURL)

		// Construct and publish the event
		port := 8080
		if p, err := strconv.Atoi(info.HostPort); err == nil {
			port = p
		}

		sandboxHost := os.Getenv("SANDBOX_HOST")
		if sandboxHost == "" {
			sandboxHost = "http://localhost"
		}

		event := model.SubmissionReadyEvent{
			SubmissionID: submission.ID,
			ContestantID: submission.TeamName,
			EndpointURL:  sandboxHost,
			Port:         port,
			Language:     submission.Language,
			SubmittedAt:  submission.SubmittedAt.Format(time.RFC3339),
			CPULimit:     2,
			MemoryLimit:  512,
			Status:       "running",
			ContainerID:  info.ContainerID,
		}

		ctx := context.Background()
		if err := publisher.PublishSubmissionReady(ctx, event); err != nil {
			log.Printf("[%s] ⚠️ Failed to publish submission.ready to Kafka: %v", submission.ID, err)
		}
	}()
}

func handleListSubmissions(c *gin.Context) {
	submissions := store.GetAllSubmissions()
	c.JSON(http.StatusOK, gin.H{
		"count":       len(submissions),
		"submissions": submissions,
	})
}

func handleGetSubmission(c *gin.Context) {
	id := c.Param("id")
	submission, found := store.GetSubmission(id)
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "Submission not found"})
		return
	}
	c.JSON(http.StatusOK, submission)
}

func handleStopSubmission(c *gin.Context) {
	id := c.Param("id")
	submission, found := store.GetSubmission(id)
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "Submission not found"})
		return
	}

	if submission.ContainerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No container running for this submission"})
		return
	}

	if err := runner.StopContainer(submission.ContainerID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	store.UpdateStatus(id, model.StatusCompleted)
	c.JSON(http.StatusOK, gin.H{"message": "Container stopped successfully"})
}

func handleContainerDeletion(c *gin.Context) {
	containerID := c.Param("containerID")
	if err := runner.StopContainer(containerID); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"status": "cleaned up container"})
}
