package main

import (
	"log"
	"sandbox-engine/api"
	"sandbox-engine/store"

	"github.com/gin-gonic/gin"
)

func main() {

	if err := store.InitStorage(); err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	router := gin.Default()

	router.MaxMultipartMemory = 50 << 20

	api.RegisterRoutes(router)

	log.Println("🚀 Sandbox Engine running on http://localhost:8080")
	router.Run(":8080")
}
