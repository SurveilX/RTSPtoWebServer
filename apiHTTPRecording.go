package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// VSSRecordingStartRequest represents the request to start VSS recording
type VSSRecordingStartRequest struct {
	RTSPURL      string `json:"rtsp_url" binding:"required"`
	UseFFmpeg    bool   `json:"use_ffmpeg,omitempty"`
	IncidentType string `json:"incident_type" binding:"required"`
	StoreID      string `json:"store_id" binding:"required"`
}

// VSSRecordingStopRequest represents the request to stop VSS recording
type VSSRecordingStopRequest struct {
	IncidentID   string `json:"incident_id" binding:"required"`
	StoreID      string `json:"store_id" binding:"required"`
	IncidentType string `json:"incident_type" binding:"required"`
	CallbackURL  string `json:"callback_url" binding:"required"`
}

// VSSRecordingResponse represents the response for VSS recording operations
type VSSRecordingResponse struct {
	Success   bool                `json:"success"`
	Message   string              `json:"message"`
	Session   *RecordingSession   `json:"session,omitempty"`
	Sessions  map[string]*RecordingSession `json:"sessions,omitempty"`
	Error     string              `json:"error,omitempty"`
	Timing    *RecordingTiming    `json:"timing,omitempty"`
}

// RecordingTiming provides estimated timing information
type RecordingTiming struct {
	EstimatedStartupTime string `json:"estimated_startup_time"`
	Method              string `json:"method"`
	Description         string `json:"description"`
}

// HTTPAPIServerStartRecording starts VSS recording for a stream channel
func HTTPAPIServerStartRecording(c *gin.Context) {
	requestLogger := log.WithFields(logrus.Fields{
		"module":  "http_vss_recording",
		"stream":  c.Param("uuid"),
		"channel": c.Param("channel"),
		"func":    "HTTPAPIServerStartRecording",
	})

	var payload VSSRecordingStartRequest
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.IndentedJSON(400, VSSRecordingResponse{
			Success: false,
			Message: "Invalid request. RTSP URL, incident_type, and store_id are required.",
			Error:   err.Error(),
		})
		requestLogger.WithFields(logrus.Fields{
			"call": "BindJSON",
		}).Errorln(err.Error())
		return
	}

	if !strings.HasPrefix(strings.ToLower(payload.RTSPURL), "rtsp://") {
		c.IndentedJSON(400, VSSRecordingResponse{
			Success: false,
			Message: "Invalid RTSP URL. Must start with rtsp://",
			Error:   "Invalid URL format",
		})
		return
	}

	if payload.IncidentType == "" || payload.StoreID == "" {
		c.IndentedJSON(400, VSSRecordingResponse{
			Success: false,
			Message: "incident_type and store_id are required for VSS recording",
			Error:   "Missing required VSS fields",
		})
		return
	}

	session, err := recordingManager.StartRecording(
		c.Param("uuid"), 
		c.Param("channel"), 
		payload.RTSPURL, 
		payload.UseFFmpeg,
		payload.IncidentType,
		payload.StoreID,
	)
	if err != nil {
		c.IndentedJSON(400, VSSRecordingResponse{
			Success: false,
			Message: "Failed to start VSS recording",
			Error:   err.Error(),
		})
		requestLogger.WithFields(logrus.Fields{
			"call": "StartRecording",
		}).Errorln(err.Error())
		return
	}

	timing := &RecordingTiming{
		Method: map[bool]string{true: "ffmpeg", false: "native"}[payload.UseFFmpeg],
	}

	if payload.UseFFmpeg {
		timing.EstimatedStartupTime = "2-5 seconds"
		timing.Description = "FFmpeg method: Spawns external process, establishes RTSP connection, and begins recording"
	} else {
		timing.EstimatedStartupTime = "1-3 seconds"
		timing.Description = "Native method: Uses Go MP4 muxer, waits for codec information, and begins recording"
	}

	c.IndentedJSON(200, VSSRecordingResponse{
		Success: true,
		Message: fmt.Sprintf("VSS recording started successfully. Files will be saved to /vss/alerts/%s/%s/ and /vss/posters/%s/%s/", 
			convertIncidentType(payload.IncidentType), payload.StoreID,
			convertIncidentType(payload.IncidentType), payload.StoreID),
		Session: session,
		Timing:  timing,
	})

	requestLogger.WithFields(logrus.Fields{
		"session_id":    session.ID,
		"method":        timing.Method,
		"incident_type": payload.IncidentType,
		"store_id":      payload.StoreID,
		"rtsp_url":      payload.RTSPURL,
	}).Infoln("VSS Recording started via API")
}

// HTTPAPIServerStopRecording stops current VSS recording and starts new one with callback
func HTTPAPIServerStopRecording(c *gin.Context) {
	requestLogger := log.WithFields(logrus.Fields{
		"module":  "http_vss_recording",
		"stream":  c.Param("uuid"),
		"channel": c.Param("channel"),
		"func":    "HTTPAPIServerStopRecording",
	})

	var payload VSSRecordingStopRequest
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.IndentedJSON(400, VSSRecordingResponse{
			Success: false,
			Message: "Invalid request. incident_id, store_id, incident_type, and callback_url are required.",
			Error:   err.Error(),
		})
		requestLogger.WithFields(logrus.Fields{
			"call": "BindJSON",
		}).Errorln(err.Error())
		return
	}

	// Validate required fields
	if payload.IncidentID == "" || payload.CallbackURL == "" || payload.StoreID == "" || payload.IncidentType == "" {
		c.IndentedJSON(400, VSSRecordingResponse{
			Success: false,
			Message: "incident_id, store_id, incident_type, and callback_url are required",
			Error:   "Missing required fields",
		})
		return
	}

	session, err := recordingManager.StopRecording(
		c.Param("uuid"), 
		c.Param("channel"), 
		payload.IncidentID, 
		payload.CallbackURL,
	)
	if err != nil {
		c.IndentedJSON(400, VSSRecordingResponse{
			Success: false,
			Message: "Failed to stop VSS recording",
			Error:   err.Error(),
		})
		requestLogger.WithFields(logrus.Fields{
			"call": "StopRecording",
		}).Errorln(err.Error())
		return
	}

	c.IndentedJSON(200, VSSRecordingResponse{
		Success: true,
		Message: fmt.Sprintf("VSS recording stopped and new recording started. Video and poster will be uploaded to Digital Ocean Spaces and callback will be sent to %s with incidentId: %s", 
			payload.CallbackURL, payload.IncidentID),
		Session: session,
	})

	requestLogger.WithFields(logrus.Fields{
		"session_id":    session.ID,
		"incident_id":   payload.IncidentID,
		"callback_url":  payload.CallbackURL,
		"store_id":      payload.StoreID,
		"incident_type": payload.IncidentType,
	}).Infoln("VSS Recording stopped and callback scheduled via API")
}

// HTTPAPIServerRemoveRecording completely stops VSS recording and removes stream
func HTTPAPIServerRemoveRecording(c *gin.Context) {
	requestLogger := log.WithFields(logrus.Fields{
		"module":  "http_vss_recording",
		"stream":  c.Param("uuid"),
		"channel": c.Param("channel"),
		"func":    "HTTPAPIServerRemoveRecording",
	})

	err := recordingManager.RemoveRecording(c.Param("uuid"), c.Param("channel"))
	if err != nil {
		c.IndentedJSON(400, VSSRecordingResponse{
			Success: false,
			Message: "Failed to remove VSS recording",
			Error:   err.Error(),
		})
		requestLogger.WithFields(logrus.Fields{
			"call": "RemoveRecording",
		}).Errorln(err.Error())
		return
	}

	c.IndentedJSON(200, VSSRecordingResponse{
		Success: true,
		Message: "VSS recording completely stopped and stream/channel removed from configuration.",
	})

	requestLogger.WithFields(logrus.Fields{
		"stream":  c.Param("uuid"),
		"channel": c.Param("channel"),
	}).Infoln("VSS Recording removed via API")
}

// HTTPAPIServerRecordingStatus gets the status of a VSS recording session
func HTTPAPIServerRecordingStatus(c *gin.Context) {
	requestLogger := log.WithFields(logrus.Fields{
		"module":  "http_vss_recording",
		"stream":  c.Param("uuid"),
		"channel": c.Param("channel"),
		"func":    "HTTPAPIServerRecordingStatus",
	})

	session, err := recordingManager.GetRecordingStatus(c.Param("uuid"), c.Param("channel"))
	if err != nil {
		c.IndentedJSON(404, VSSRecordingResponse{
			Success: false,
			Message: "VSS recording session not found",
			Error:   err.Error(),
		})
		requestLogger.WithFields(logrus.Fields{
			"call": "GetRecordingStatus",
		}).Errorln(err.Error())
		return
	}

	c.IndentedJSON(200, VSSRecordingResponse{
		Success: true,
		Message: "VSS recording status retrieved",
		Session: session,
	})
}

// HTTPAPIServerListRecordings lists all VSS recording sessions
func HTTPAPIServerListRecordings(c *gin.Context) {
	sessions := recordingManager.ListRecordingSessions()

	c.IndentedJSON(200, VSSRecordingResponse{
		Success:  true,
		Message:  "VSS recording sessions retrieved",
		Sessions: sessions,
	})
}

// HTTPAPIServerDownloadRecording allows downloading a VSS recording file
func HTTPAPIServerDownloadRecording(c *gin.Context) {
	sessionID := c.Param("session_id")

	var targetSession *RecordingSession
	sessions := recordingManager.ListRecordingSessions()
	
	for _, session := range sessions {
		if session.ID == sessionID {
			targetSession = session
			break
		}
	}
	
	if targetSession == nil {
		c.IndentedJSON(404, VSSRecordingResponse{
			Success: false,
			Message: "VSS recording session not found",
			Error:   "Invalid session ID",
		})
		return
	}

	if _, err := os.Stat(targetSession.FilePath); os.IsNotExist(err) {
		c.IndentedJSON(404, VSSRecordingResponse{
			Success: false,
			Message: "VSS recording file not found on local storage. It may have been uploaded to Digital Ocean Spaces.",
			Error:   "File does not exist on disk",
		})
		return
	}

	fileName := filepath.Base(targetSession.FilePath)
	c.Header("Content-Description", "File Transfer")
	c.Header("Content-Transfer-Encoding", "binary")
	c.Header("Content-Disposition", "attachment; filename="+fileName)
	c.Header("Content-Type", "video/mp4")

	c.File(targetSession.FilePath)
}

// HTTPAPIServerDeleteRecording deletes a VSS recording file and session
func HTTPAPIServerDeleteRecording(c *gin.Context) {
	sessionID := c.Param("session_id")

	var targetSession *RecordingSession
	sessions := recordingManager.ListRecordingSessions()
	
	for _, session := range sessions {
		if session.ID == sessionID {
			targetSession = session
			break
		}
	}
	
	if targetSession == nil {
		c.IndentedJSON(404, VSSRecordingResponse{
			Success: false,
			Message: "VSS recording session not found",
			Error:   "Invalid session ID",
		})
		return
	}

	if targetSession.Status == "recording" {
		recordingManager.StopRecording(targetSession.StreamID, targetSession.ChannelID, "", "")
	}

	if err := os.Remove(targetSession.FilePath); err != nil && !os.IsNotExist(err) {
		c.IndentedJSON(500, VSSRecordingResponse{
			Success: false,
			Message: "Failed to delete local VSS recording file",
			Error:   err.Error(),
		})
		return
	}

	os.Remove(targetSession.PosterPath)

	recordingManager.CleanupSession(targetSession.StreamID, targetSession.ChannelID)

	c.IndentedJSON(200, VSSRecordingResponse{
		Success: true,
		Message: "Local VSS recording files deleted successfully. Note: Files may still exist in Digital Ocean Spaces.",
	})

	log.WithFields(logrus.Fields{
		"module":     "recording",
		"session_id": sessionID,
	}).Infoln("VSS Recording deleted")
}
