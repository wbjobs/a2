package handler

import (
	"net/http"
	"rs-service/internal/db"
	"rs-service/internal/service"
	"strconv"

	"github.com/gin-gonic/gin"
)

type FileHandler struct {
	svc *service.Service
}

func NewFileHandler(svc *service.Service) *FileHandler {
	return &FileHandler{svc: svc}
}

func (h *FileHandler) Upload(c *gin.Context) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to get file: " + err.Error()})
		return
	}
	defer file.Close()

	result, err := h.svc.UploadFile(header.Filename, file, header.Size)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "upload failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "success",
		"data":    result,
	})
}

func (h *FileHandler) List(c *gin.Context) {
	files, err := db.ListFiles()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "list files failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "success",
		"data":    files,
	})
}

func (h *FileHandler) Get(c *gin.Context) {
	fileID := c.Param("id")
	file, err := db.GetFile(fileID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "file not found: " + err.Error()})
		return
	}

	shards, err := db.GetShardsByFile(fileID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "get shards failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "success",
		"data": gin.H{
			"file":   file,
			"shards": shards,
		},
	})
}

func (h *FileHandler) Download(c *gin.Context) {
	fileID := c.Param("id")

	fileName, reader, size, err := h.svc.GetFileReader(fileID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "download failed: " + err.Error()})
		return
	}
	defer reader.Close()

	c.Header("Content-Disposition", "attachment; filename="+fileName)
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Length", strconv.FormatInt(size, 10))

	c.DataFromReader(http.StatusOK, size, "application/octet-stream", reader, nil)
}

func (h *FileHandler) GetShards(c *gin.Context) {
	fileID := c.Param("id")
	shards, err := db.GetShardsByFile(fileID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "get shards failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "success",
		"data":    shards,
	})
}
