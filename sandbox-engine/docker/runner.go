package docker

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Runner struct {
	mu         sync.Mutex
	portOffset int
}

type ContainerInfo struct {
	ContainerID string
	HostPort    string
	EndpointURL string
}

func NewRunner() (*Runner, error) {
	cmd := exec.Command("docker", "info")
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("Docker is not running or not accessible: %w", err)
	}
	return &Runner{portOffset: 0}, nil
}

// CleanupManagedContainers removes any orphaned sandbox containers from previous runs.
// Called on startup to prevent resource leaks if the engine crashed mid-run.
func (r *Runner) CleanupManagedContainers() {
	cmd := exec.Command("docker", "ps", "-q", "--filter", "label=sandbox.managed=true")
	out, err := cmd.Output()
	if err != nil {
		log.Printf("[cleanup] failed to list managed containers: %v", err)
		return
	}
	ids := strings.Fields(strings.TrimSpace(string(out)))
	if len(ids) == 0 {
		log.Printf("[cleanup] no orphaned sandbox containers found")
		return
	}
	for _, id := range ids {
		log.Printf("[cleanup] removing orphaned container %s", id)
		_ = r.StopContainer(id)
	}
}

// removeImage attempts to remove a Docker image; errors are logged but not fatal.
func (r *Runner) removeImage(imageName string) {
	cmd := exec.Command("docker", "rmi", "-f", imageName)
	if err := cmd.Run(); err != nil {
		log.Printf("[cleanup] failed to remove image %s: %v", imageName, err)
	} else {
		log.Printf("[cleanup] removed image %s", imageName)
	}
}

func (r *Runner) DeploySubmission(submissionID, zipPath, language string) (*ContainerInfo, error) {
	extractDir, err := extractZip(zipPath, submissionID)
	if err != nil {
		return nil, fmt.Errorf("failed to extract zip: %w", err)
	}
	defer os.RemoveAll(extractDir)

	langMap := map[string]string{
		"cpp":  "cplusplus",
		"go":   "golang",
		"rust": "rust",
	}
	dockerfileName, ok := langMap[language]
	if !ok {
		return nil, fmt.Errorf("unsupported language: %s", language)
	}

	dockerfileSrc := fmt.Sprintf("./docker/Dockerfile.%s", dockerfileName)
	dockerfileDst := filepath.Join(extractDir, "Dockerfile")
	if err := copyFile(dockerfileSrc, dockerfileDst); err != nil {
		return nil, fmt.Errorf("failed to copy Dockerfile: %w", err)
	}

	imageName := fmt.Sprintf("submission-%s", submissionID)
	log.Printf("[%s] Building Docker image...", submissionID)
	if err := r.buildImage(extractDir, imageName); err != nil {
		r.removeImage(imageName) // clean up partial image on build failure
		return nil, fmt.Errorf("build failed: %w", err)
	}

	r.mu.Lock()
	hostPort := fmt.Sprintf("%d", 9000+r.portOffset)
	r.portOffset++
	r.mu.Unlock()

	log.Printf("[%s] Starting container on port %s...", submissionID, hostPort)
	containerID, err := r.runContainer(imageName, hostPort, submissionID)
	if err != nil {
		r.removeImage(imageName) // clean up image if container failed to start
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	sandboxHost := os.Getenv("SANDBOX_HOST")
	if sandboxHost == "" {
		sandboxHost = "http://localhost"
	}
	sandboxHost = strings.TrimSuffix(sandboxHost, "/")

	return &ContainerInfo{
		ContainerID: containerID,
		HostPort:    hostPort,
		EndpointURL: fmt.Sprintf("%s:%s", sandboxHost, hostPort),
	}, nil
}

func (r *Runner) buildImage(contextDir, imageName string) error {
	// 300-second timeout prevents malicious Dockerfiles from hanging the engine
	// Note: Set higher than 120s to allow time for pulling large base images initially
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "build", "--force-rm", "-t", imageName, contextDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("docker build timed out after 300s")
		}
		return fmt.Errorf("docker build failed: %w", err)
	}
	return nil
}

func (r *Runner) runContainer(imageName, hostPort, submissionID string) (string, error) {
	args := []string{
		"run", "-d",
		"-p", fmt.Sprintf("%s:8080", hostPort),
		"--memory", "512m",
		"--cpus", "2",
		"--pids-limit", "64",
		"--cap-drop", "ALL",
		"--security-opt", "no-new-privileges",
		"--read-only",
		"--network", "bridge",
		"--label", fmt.Sprintf("sandbox.submission_id=%s", submissionID),
		"--label", "sandbox.managed=true",
		imageName,
	}

	cmd := exec.Command("docker", args...)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("docker run failed: %w", err)
	}

	containerID := strings.TrimSpace(string(out))
	log.Printf("[%s] Container started: %s", submissionID, containerID[:12])
	return containerID, nil
}

func (r *Runner) StopContainer(containerID string) error {
	stopCmd := exec.Command("docker", "stop", containerID)
	if err := stopCmd.Run(); err != nil {
		return fmt.Errorf("docker stop failed: %w", err)
	}

	rmCmd := exec.Command("docker", "rm", containerID)
	if err := rmCmd.Run(); err != nil {
		return fmt.Errorf("docker rm failed: %w", err)
	}

	log.Printf("Container %s stopped and removed", containerID[:12])
	return nil
}

// safePath resolves a zip/tar entry name into a safe absolute path under destDir.
// Returns empty string if the path escapes the destination (Zip Slip attack).
func safePath(destDir, entryName string) string {
	// Clean the entry name to prevent path traversal
	cleaned := filepath.Clean(entryName)
	if strings.HasPrefix(cleaned, "..") || strings.HasPrefix(cleaned, string(filepath.Separator)) {
		return ""
	}
	full := filepath.Join(destDir, cleaned)
	// Double-check the resolved path is under destDir
	if !strings.HasPrefix(filepath.Clean(full), filepath.Clean(destDir)) {
		return ""
	}
	return full
}

func extractZip(zipPath, id string) (string, error) {
	destDir := filepath.Join(os.TempDir(), "sandbox-"+id)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return "", err
	}

	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return extractTarGz(zipPath, destDir)
	}
	defer r.Close()

	for _, f := range r.File {
		fPath := safePath(destDir, f.Name)
		if fPath == "" {
			log.Printf("[extract] skipping unsafe path: %s", f.Name)
			continue
		}
		if f.FileInfo().IsDir() {
			os.MkdirAll(fPath, 0755)
			continue
		}
		// Ensure parent directory exists for nested files like src/main.rs
		os.MkdirAll(filepath.Dir(fPath), 0755)
		rc, err := f.Open()
		if err != nil {
			return "", err
		}
		outFile, err := os.Create(fPath)
		if err != nil {
			rc.Close()
			return "", err
		}
		io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()
	}
	return destDir, nil
}

func extractTarGz(src, destDir string) (string, error) {
	f, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", err
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		fPath := safePath(destDir, hdr.Name)
		if fPath == "" {
			log.Printf("[extract] skipping unsafe path: %s", hdr.Name)
			continue
		}
		if hdr.FileInfo().IsDir() {
			os.MkdirAll(fPath, 0755)
			continue
		}
		os.MkdirAll(filepath.Dir(fPath), 0755)
		outFile, err := os.Create(fPath)
		if err != nil {
			return "", err
		}
		io.Copy(outFile, tr)
		outFile.Close()
	}
	return destDir, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}
