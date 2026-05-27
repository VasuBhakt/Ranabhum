package main

import (
	"log"
	"sandbox-engine/api"
	"sandbox-engine/docker"
	"sandbox-engine/store"

	"github.com/gin-gonic/gin"
)

func main() {
	if err := store.InitStorage(); err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	runner, err := docker.NewRunner()
	if err != nil {
		log.Fatalf("Failed to connect to Docker: %v", err)
	}
	log.Println("🐳 Connected to Docker successfully")

	api.InitRunner(runner)

	router := gin.Default()
	router.MaxMultipartMemory = 50 << 20
	api.RegisterRoutes(router)

	log.Println("🚀 Sandbox Engine running on http://localhost:8080")
	router.Run(":8080")
}
