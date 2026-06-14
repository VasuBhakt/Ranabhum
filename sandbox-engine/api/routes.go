package api

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sandbox-engine/docker"
	"sandbox-engine/model"
	"sandbox-engine/publisher"
	"sandbox-engine/store"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

var runner *docker.Runner

// proxyCache stores a reusable *httputil.ReverseProxy per submission ID.
// Each proxy has its own http.Transport with keep-alive and connection pooling,
// so thousands of bot requests reuse a small pool of persistent TCP connections
// to the container instead of opening (and immediately closing) one per request.
var proxyCache sync.Map // map[submissionID string]*httputil.ReverseProxy

func InitRunner(r *docker.Runner) {
	runner = r
}

func RegisterRoutes(router *gin.Engine) {
	router.POST("/submit", handleSubmit)
	router.GET("/submissions", handleListSubmissions)
	router.GET("/submissions/:id", handleGetSubmission)
	router.DELETE("/submissions/:id", handleStopSubmission)
	router.DELETE("/sandbox/:containerID", handleContainerDeletion)
	router.POST("/sandbox/:id/order", handleProxyOrder)
}

// getOrCreateProxy returns a cached reverse proxy for the given submission,
// creating one on first access. The proxy's Transport is configured for
// high-throughput keep-alive connections to the sandbox container.
func getOrCreateProxy(submissionID, endpointURL string) (*httputil.ReverseProxy, error) {
	if cached, ok := proxyCache.Load(submissionID); ok {
		return cached.(*httputil.ReverseProxy), nil
	}

	target, err := url.Parse(endpointURL)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint URL %q: %w", endpointURL, err)
	}

	// Dedicated transport per container — keeps connections alive and pools them.
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        5000,
		MaxIdleConnsPerHost: 5000, // perfectly matches botfleet's capacity
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.URL.Path = "/order"
			req.Host = target.Host
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("[proxy:%s] upstream error: %v", submissionID, err)
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(`{"error":"upstream unavailable"}`))
		},
	}

	// Store-if-absent to handle concurrent first requests safely
	actual, _ := proxyCache.LoadOrStore(submissionID, proxy)
	return actual.(*httputil.ReverseProxy), nil
}

// evictProxy removes the cached proxy for a submission and closes its idle connections.
func evictProxy(submissionID string) {
	if cached, ok := proxyCache.LoadAndDelete(submissionID); ok {
		if p, ok := cached.(*httputil.ReverseProxy); ok {
			if t, ok := p.Transport.(*http.Transport); ok {
				t.CloseIdleConnections()
			}
		}
		log.Printf("[proxy:%s] evicted from cache", submissionID)
	}
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

		proxyURL := fmt.Sprintf("%s:%d/sandbox/%s", sandboxHost, port, submission.ID)

		event := model.SubmissionReadyEvent{
			SubmissionID: submission.ID,
			ContestantID: submission.TeamName,
			EndpointURL:  proxyURL,
			Port:         0,
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

	evictProxy(id)
	store.UpdateStatus(id, model.StatusCompleted)
	c.JSON(http.StatusOK, gin.H{"message": "Container stopped successfully"})
}

func handleContainerDeletion(c *gin.Context) {
	containerID := c.Param("containerID")

	// Find submission by containerID so we can evict its proxy
	for _, s := range store.GetAllSubmissions() {
		if s.ContainerID == containerID {
			evictProxy(s.ID)
			break
		}
	}

	if err := runner.StopContainer(containerID); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"status": "cleaned up container"})
}

func handleProxyOrder(c *gin.Context) {
	id := c.Param("id")
	submission, found := store.GetSubmission(id)
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "Submission not found"})
		return
	}

	proxy, err := getOrCreateProxy(id, submission.EndpointURL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Invalid endpoint URL"})
		return
	}

	proxy.ServeHTTP(c.Writer, c.Request)
}
