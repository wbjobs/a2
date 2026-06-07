package handler

import (
	"fmt"
	"net/http"
	"rs-service/internal/db"
	"rs-service/internal/erasure"
	"rs-service/internal/service"

	"github.com/gin-gonic/gin"
)

type RebuildHandler struct {
	svc *service.Service
}

func NewRebuildHandler(svc *service.Service) *RebuildHandler {
	return &RebuildHandler{svc: svc}
}

type RebuildRequest struct {
	FileID        string `json:"file_id" binding:"required"`
	FailedNodeIDs []int  `json:"failed_node_ids" binding:"required,min=1"`
}

func (h *RebuildHandler) Rebuild(c *gin.Context) {
	var req RebuildRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: " + err.Error()})
		return
	}

	if len(req.FailedNodeIDs) > erasure.ParityShards {
		c.JSON(http.StatusConflict, gin.H{
			"code":    409,
			"message": "too many failed nodes",
			"data": gin.H{
				"failed_nodes":   len(req.FailedNodeIDs),
				"max_recoverable": erasure.ParityShards,
				"error":         fmt.Sprintf("cannot recover from %d node failures, maximum recoverable is %d", len(req.FailedNodeIDs), erasure.ParityShards),
			},
		})
		return
	}

	if _, err := db.GetFile(req.FileID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "file not found: " + err.Error()})
		return
	}

	result, err := h.svc.RebuildFile(req.FileID, req.FailedNodeIDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "rebuild failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "rebuild completed",
		"data":    result,
	})
}

type RebuildNodeRequest struct {
	NodeID int `json:"node_id" binding:"required,min=0,max=8"`
}

func (h *RebuildHandler) RebuildByNode(c *gin.Context) {
	var req RebuildNodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: " + err.Error()})
		return
	}

	fileIDs, err := h.svc.GetFilesAffectedByNode(req.NodeID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "get affected files failed: " + err.Error()})
		return
	}

	if len(fileIDs) == 0 {
		c.JSON(http.StatusOK, gin.H{
			"code":    0,
			"message": "no files affected by this node",
			"data": gin.H{
				"node_id":        req.NodeID,
				"files_rebuilt":  0,
				"files_failed":   0,
				"results":        []interface{}{},
			},
		})
		return
	}

	results := make([]*service.RebuildResult, 0, len(fileIDs))
	failedNodes := []int{req.NodeID}
	successCount := 0
	failCount := 0

	for _, fileID := range fileIDs {
		result, err := h.svc.RebuildFile(fileID, failedNodes)
		if err != nil {
			failCount++
			result = &service.RebuildResult{
				FileID:       fileID,
				Status:       db.RebuildStatusFailed,
				ErrorMessage: err.Error(),
			}
		} else if result.Status == db.RebuildStatusSuccess {
			successCount++
		} else {
			failCount++
		}
		results = append(results, result)
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "node rebuild completed",
		"data": gin.H{
			"node_id":       req.NodeID,
			"files_rebuilt": successCount,
			"files_failed":  failCount,
			"results":       results,
		},
	})
}

func (h *RebuildHandler) ListLogs(c *gin.Context) {
	fileID := c.Query("file_id")

	logs, err := db.ListRebuildLogs(fileID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "list rebuild logs failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "success",
		"data":    logs,
	})
}

func (h *RebuildHandler) GetLog(c *gin.Context) {
	fileID := c.Param("file_id")

	logs, err := db.ListRebuildLogs(fileID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "get rebuild logs failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "success",
		"data":    logs,
	})
}
