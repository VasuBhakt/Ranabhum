package api

import (
	"net/http"
	"sandbox-engine/model"
	"sandbox-engine/store"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(router *gin.Engine) {
	router.POST("/submit", handleSubmit)
	router.GET("/submissions", handleListSubmissions)
	router.GET("/submissions/:id", handleGetSubmission)
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
		"message":    "Submission received successfully",
		"submission": submission,
	})
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
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Submission not found",
		})
		return
	}
	c.JSON(http.StatusOK, submission)
}

var _ = model.Submission{}
