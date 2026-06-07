package main

import (
	"log"
	"rs-service/internal/db"
	"rs-service/internal/erasure"
	"rs-service/internal/handler"
	"rs-service/internal/service"
	"rs-service/internal/storage"

	"github.com/gin-gonic/gin"
)

func main() {
	if err := db.Init("data/metadata.db"); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()
	log.Println("Database initialized successfully")

	enc, err := erasure.NewEncoder()
	if err != nil {
		log.Fatalf("Failed to create erasure encoder: %v", err)
	}
	log.Println("Reed-Solomon encoder initialized (6 data + 3 parity shards)")

	store, err := storage.NewStore("nodes")
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}
	log.Println("Storage initialized with 9 virtual nodes")

	svc := service.NewService(enc, store)

	fileHandler := handler.NewFileHandler(svc)
	nodeHandler := handler.NewNodeHandler(svc)
	rebuildHandler := handler.NewRebuildHandler(svc)

	r := gin.Default()

	api := r.Group("/api/v1")
	{
		files := api.Group("/files")
		{
			files.POST("", fileHandler.Upload)
			files.GET("", fileHandler.List)
			files.GET("/:id", fileHandler.Get)
			files.GET("/:id/download", fileHandler.Download)
			files.GET("/:id/shards", fileHandler.GetShards)
		}

		nodes := api.Group("/nodes")
		{
			nodes.GET("", nodeHandler.List)
			nodes.GET("/:id", nodeHandler.Get)
			nodes.PUT("/:id/status", nodeHandler.SetStatus)
			nodes.POST("/:id/offline", nodeHandler.MarkOffline)
			nodes.POST("/:id/online", nodeHandler.MarkOnline)
		}

		rebuild := api.Group("/rebuild")
		{
			rebuild.POST("", rebuildHandler.Rebuild)
			rebuild.POST("/node", rebuildHandler.RebuildByNode)
			rebuild.GET("/logs", rebuildHandler.ListLogs)
			rebuild.GET("/logs/:file_id", rebuildHandler.GetLog)
		}
	}

	log.Println("Server starting on :8080")
	log.Println("API Endpoints:")
	log.Println("  POST   /api/v1/files               - Upload file")
	log.Println("  GET    /api/v1/files               - List files")
	log.Println("  GET    /api/v1/files/:id           - Get file info")
	log.Println("  GET    /api/v1/files/:id/download  - Download file")
	log.Println("  GET    /api/v1/files/:id/shards    - Get file shards")
	log.Println("  GET    /api/v1/nodes               - List nodes")
	log.Println("  GET    /api/v1/nodes/:id           - Get node info")
	log.Println("  POST   /api/v1/nodes/:id/offline   - Mark node offline")
	log.Println("  POST   /api/v1/nodes/:id/online    - Mark node online")
	log.Println("  PUT    /api/v1/nodes/:id/status    - Set node status")
	log.Println("  POST   /api/v1/rebuild             - Rebuild file")
	log.Println("  POST   /api/v1/rebuild/node        - Rebuild by node")
	log.Println("  GET    /api/v1/rebuild/logs        - List rebuild logs")
	log.Println("  GET    /api/v1/rebuild/logs/:file_id - Get rebuild logs for a file")

	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
