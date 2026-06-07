package handler

import (
	"net/http"
	"rs-service/internal/db"
	"rs-service/internal/service"
	"strconv"

	"github.com/gin-gonic/gin"
)

type NodeHandler struct {
	svc *service.Service
}

func NewNodeHandler(svc *service.Service) *NodeHandler {
	return &NodeHandler{svc: svc}
}

func (h *NodeHandler) List(c *gin.Context) {
	nodes, err := db.GetNodes()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "list nodes failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "success",
		"data":    nodes,
	})
}

func (h *NodeHandler) Get(c *gin.Context) {
	nodeID, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid node id"})
		return
	}

	node, err := db.GetNode(nodeID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "node not found: " + err.Error()})
		return
	}

	fileIDs, err := h.svc.GetFilesAffectedByNode(nodeID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "get affected files failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "success",
		"data": gin.H{
			"node":          node,
			"affected_files": fileIDs,
		},
	})
}

type SetStatusRequest struct {
	Status string `json:"status" binding:"required,oneof=online offline"`
}

func (h *NodeHandler) SetStatus(c *gin.Context) {
	nodeID, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid node id"})
		return
	}

	var req SetStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: " + err.Error()})
		return
	}

	_, err = db.GetNode(nodeID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "node not found: " + err.Error()})
		return
	}

	status := db.NodeStatusOnline
	if req.Status == "offline" {
		status = db.NodeStatusOffline
	}

	if err := db.SetNodeStatus(nodeID, status); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "set node status failed: " + err.Error()})
		return
	}

	fileIDs, err := h.svc.GetFilesAffectedByNode(nodeID)
	if err != nil {
		fileIDs = []string{}
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "success",
		"data": gin.H{
			"node_id":         nodeID,
			"status":          status,
			"affected_files":  fileIDs,
		},
	})
}

func (h *NodeHandler) MarkOffline(c *gin.Context) {
	nodeID, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid node id"})
		return
	}

	_, err = db.GetNode(nodeID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "node not found: " + err.Error()})
		return
	}

	if err := db.SetNodeStatus(nodeID, db.NodeStatusOffline); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "mark node offline failed: " + err.Error()})
		return
	}

	fileIDs, err := h.svc.GetFilesAffectedByNode(nodeID)
	if err != nil {
		fileIDs = []string{}
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "node marked as offline",
		"data": gin.H{
			"node_id":        nodeID,
			"status":         db.NodeStatusOffline,
			"affected_files": fileIDs,
		},
	})
}

func (h *NodeHandler) MarkOnline(c *gin.Context) {
	nodeID, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid node id"})
		return
	}

	_, err = db.GetNode(nodeID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "node not found: " + err.Error()})
		return
	}

	if err := db.SetNodeStatus(nodeID, db.NodeStatusOnline); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "mark node online failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "node marked as online",
		"data": gin.H{
			"node_id": nodeID,
			"status":  db.NodeStatusOnline,
		},
	})
}
