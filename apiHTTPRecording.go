package main

import (
	"time"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// VSSRecordingStartRequest represents the request to start recording (no incident details yet)
type VSSRecordingStartRequest struct {
	RTSPURL   string `json:"rtsp_url"`
	UseFFmpeg bool   `json:"use_ffmpeg,omitempty"`
	FPS       int    `json:"fps,omitempty"`
	Codec     string `json:"codec,omitempty"`
}

// VSSRecordingStopRequest represents the request to stop recording with incident details
type VSSRecordingStopRequest struct {
	IncidentID   string `json:"incident_id,omitempty"`
	StoreID      string `json:"store_id,omitempty"`
	IncidentType string `json:"incident_type,omitempty"`
	CallbackURL  string `json:"callback_url,omitempty"`
	IsStartNext  bool   `json:"is_start_next,omitempty"`
}

// VSSRecordingUploadRequest represents the request to upload a recording
type VSSRecordingUploadRequest struct {
	IncidentID   string `json:"incident_id" binding:"required"`
	StoreID      string `json:"store_id" binding:"required"`
	IncidentType string `json:"incident_type" binding:"required"`
	CallbackURL  string `json:"callback_url" binding:"required"`
}

// VSSRecordingResponse represents the response for recording operations
type VSSRecordingResponse struct {
	Success   bool                `json:"success"`
	Message   string              `json:"message"`
	Session   *RecordingSession   `json:"session,omitempty"`
	NewSession *RecordingSession  `json:"new_session,omitempty"`
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

// HTTPAPIServerStartRecording starts recording for a stream channel (no incident details yet)
func HTTPAPIServerStartRecording(c *gin.Context) {
	streamID := c.Param("uuid")
	channelID := c.Param("channel")

	requestLogger := log.WithFields(logrus.Fields{
		"module":  "http_recording",
		"stream":  streamID,
		"channel": channelID,
		"func":    "HTTPAPIServerStartRecording",
	})

	var payload VSSRecordingStartRequest
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.IndentedJSON(400, VSSRecordingResponse{
			Success: false,
			Message: "Invalid request. RTSP URL is required.",
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

	if !Storage.StreamExist(streamID) {
		requestLogger.Infoln("Stream does not exist, creating new stream")
		err := Storage.StreamAdd(streamID, StreamST{
			Name: "Auto-created stream for recording",
			Channels: make(map[string]ChannelST),
		})
		if err != nil {
			c.IndentedJSON(500, VSSRecordingResponse{
				Success: false,
				Message: "Failed to create stream",
				Error:   err.Error(),
			})
			requestLogger.WithFields(logrus.Fields{"call": "StreamAdd"}).Errorln(err.Error())
			return
		}
	} else if !Storage.StreamChannelExist(streamID, channelID) {
		requestLogger.Infoln("Channel does not exist, creating new channel")
		err := Storage.StreamChannelAdd(streamID, channelID, ChannelST{
			Name:     "Auto-created channel for recording",
			URL:      payload.RTSPURL,
			OnDemand: true,
			Debug:    false,
			Audio:    false,
		})
		if err != nil {
			c.IndentedJSON(500, VSSRecordingResponse{
				Success: false,
				Message: "Failed to create channel",
				Error:   err.Error(),
			})
			requestLogger.WithFields(logrus.Fields{"call": "StreamChannelAdd"}).Errorln(err.Error())
			return
		}
	}

	session, err := recordingManager.StartRecording(
		c.Param("uuid"), 
		c.Param("channel"), 
		payload.RTSPURL, 
		payload.UseFFmpeg,
		"",
		"",
		payload.FPS,
		payload.Codec,
	)

	if err != nil {
		c.IndentedJSON(400, VSSRecordingResponse{
			Success: false,
			Message: "Failed to start recording",
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
		Message: "Recording started successfully. Files will be organized into VSS structure when stopped with incident details.",
		Session: session,
		Timing:  timing,
	})

	requestLogger.WithFields(logrus.Fields{
		"session_id":     session.ID,
		"method":         timing.Method,
		"rtsp_url":       payload.RTSPURL,
		"fps":            session.FPS,
		"codec":          session.Codec,
		"start_duration": session.StartDuration.String(),
	}).Infoln("Recording started via API (no incident details yet)")
}

// HTTPAPIStopAndStart handles stop and start request
func HTTPAPIServerStopAndStart(c *gin.Context) {
	requestStart := time.Now()
	streamID := c.Param("uuid")
	channelID := c.Param("channel")

	requestLogger := log.WithFields(logrus.Fields{
		"module":     "recording_api",
		"action":     "fast_stop_start",
		"stream_id":  streamID,
		"channel_id": channelID,
	})

	requestLogger.WithFields(logrus.Fields{
		"step": "api_entry",
	}).Infoln("HTTPAPIServerStopAndStart: API request received")

	requestLogger.WithFields(logrus.Fields{
		"step": "call_manager",
	}).Infoln("HTTPAPIServerStopAndStart: Calling recordingManager.StopAndStart")

	oldSession, newSession, err := recordingManager.StopAndStart(streamID, channelID)
	
	requestLogger.WithFields(logrus.Fields{
		"step":     "manager_returned",
		"duration": time.Since(requestStart).String(),
		"error":    err,
	}).Infoln("HTTPAPIServerStopAndStart: recordingManager.StopAndStart returned")
	
	if err != nil {
		requestLogger.WithFields(logrus.Fields{
			"step":  "error_response",
			"error": err.Error(),
		}).Errorln("HTTPAPIServerStopAndStart: Failed to stop and start recording")
		
		c.IndentedJSON(500, VSSRecordingResponse{
			Success: false,
			Message: "Failed to stop and start recording",
			Error:   err.Error(),
		})
		return
	}

	requestLogger.WithFields(logrus.Fields{
		"step": "build_response",
	}).Infoln("HTTPAPIServerStopAndStart: Building response")

	response := VSSRecordingResponse{
		Success:    true,
		Message:    "Recording stop and start completed successfully",
		Session:    oldSession,
		NewSession: newSession,
	}

	requestLogger.WithFields(logrus.Fields{
		"step": "send_response",
	}).Infoln("HTTPAPIServerStopAndStart: Sending response")

	c.IndentedJSON(200, response)

	requestLogger.WithFields(logrus.Fields{
		"old_session_id": func() string { if oldSession != nil { return oldSession.ID } else { return "none" } }(),
		"new_session_id": func() string { if newSession != nil { return newSession.ID } else { return "none" } }(),
		"stop_duration":  func() string { if oldSession != nil { return oldSession.StopDuration.String() } else { return "n/a" } }(),
		"start_duration": func() string { if newSession != nil { return newSession.StartDuration.String() } else { return "n/a" } }(),
		"total_duration": time.Since(requestStart).String(),
		"step":           "api_complete",
	}).Infoln("HTTPAPIServerStopAndStart: Stop and start completed successfully")
}

// HTTPAPIUploadRecording handles the upload recording request
func HTTPAPIServerUploadRecording(c *gin.Context) {
	streamID := c.Param("uuid")
	channelID := c.Param("channel")

	requestLogger := log.WithFields(logrus.Fields{
		"module": "recording_api",
		"action": "upload",
		"stream_id": streamID,
		"channel_id": channelID,
	})

	requestLogger.Infoln("Upload recording request")

	var payload VSSRecordingUploadRequest
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.IndentedJSON(400, VSSRecordingResponse{
			Success: false,
			Message: "Invalid JSON format in request body.",
			Error:   err.Error(),
		})
		requestLogger.WithFields(logrus.Fields{"call": "BindJSON"}).Errorln(err.Error())
		return
	}

	payload.IncidentID = strings.TrimSpace(payload.IncidentID)
	payload.StoreID = strings.TrimSpace(payload.StoreID)
	payload.IncidentType = strings.TrimSpace(payload.IncidentType)
	payload.CallbackURL = strings.TrimSpace(payload.CallbackURL)

	session, err := recordingManager.GetRecordingStatus(streamID, channelID)
	if err != nil {
		c.IndentedJSON(404, VSSRecordingResponse{
			Success: false,
			Message: "No active recording session found for this stream/channel",
			Error:   err.Error(),
		})
		requestLogger.WithFields(logrus.Fields{"call": "GetRecordingStatus"}).Errorln(err.Error())
		return
	}

	requestLogger.WithFields(logrus.Fields{
		"session_id":    session.ID,
		"incident_id":   payload.IncidentID,
		"store_id":      payload.StoreID,
		"incident_type": payload.IncidentType,
		"callback_url":  payload.CallbackURL,
	}).Debugln("Processing upload recording request")

	if err := recordingManager.QueueUpload(session.ID, session.FilePath, session.PosterPath, payload.IncidentID, payload.IncidentType, payload.StoreID, payload.CallbackURL); err != nil {
		c.IndentedJSON(500, VSSRecordingResponse{
			Success: false,
			Message: "Failed to queue upload",
			Error:   err.Error(),
		})
		requestLogger.WithFields(logrus.Fields{"call": "QueueUpload"}).Errorln(err.Error())
		return
	}

	c.IndentedJSON(200, VSSRecordingResponse{
		Success: true,
		Message: "Upload queued for background processing",
	})

	requestLogger.WithFields(logrus.Fields{
		"session_id":    session.ID,
		"incident_id":   payload.IncidentID,
		"incident_type": payload.IncidentType,
		"store_id":      payload.StoreID,
	}).Infoln("Upload queued successfully")
}

// HTTPAPIServerStopRecording stops current recording and organizes with incident details
func HTTPAPIServerStopRecording(c *gin.Context) {
	streamID := c.Param("uuid")
	channelID := c.Param("channel")

	requestLogger := log.WithFields(logrus.Fields{
		"module":  "http_recording",
		"stream":  streamID,
		"channel": channelID,
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

	hasIncidentDetails := payload.IncidentID != "" && payload.StoreID != "" && 
		payload.IncidentType != "" && payload.CallbackURL != ""

	var session *RecordingSession
	var newSession *RecordingSession
	var err error
	var message string

	if hasIncidentDetails {
		if payload.IsStartNext {
			session, newSession, err = recordingManager.StopRecordingWithNext(
				streamID, channelID, payload.IncidentID, payload.StoreID, payload.IncidentType, payload.CallbackURL, true)
			if err != nil {
				if session == nil && newSession == nil {
					c.IndentedJSON(200, VSSRecordingResponse{
						Success: true,
						Message: "No active recording found for stream, new recording started",
					})
					requestLogger.Warnln("No active recording found")
					return
				}
			}
			message = "Recording stopped and organized into VSS structure. Files queued for upload. New recording started automatically"
			if newSession != nil {
				message += " (session: " + newSession.ID + ")"
			}
		} else {
			session, _, err = recordingManager.StopRecordingWithNext(
				streamID, channelID, payload.IncidentID, payload.StoreID, payload.IncidentType, payload.CallbackURL, false)
			if err != nil {
				if session == nil {
					c.IndentedJSON(200, VSSRecordingResponse{
						Success: true,
						Message: "No active recording found for stream",
					})
					requestLogger.Warnln("No active recording found")
					return
				}
			}
			message = "Recording stopped and organized into VSS structure. Files queued for upload."
		}
	} else {
		if payload.IsStartNext {
			session, newSession, err = recordingManager.StopRecordingNoIncidentWithNext(streamID, channelID, true)
			if err != nil {
				if session == nil && newSession == nil {
					c.IndentedJSON(200, VSSRecordingResponse{
						Success: true,
						Message: "No active recording found for stream, new recording started",
					})
					requestLogger.Warnln("No active recording found")
					return
				}
			}
			message = "Recording stopped and temporary files cleaned up. New recording started automatically"
			if newSession != nil {
				message += " (session: " + newSession.ID + ")"
			}
		} else {
			session, _, err := recordingManager.StopRecordingNoIncidentWithNext(streamID, channelID, false)
			if err != nil {
				if session == nil {
					c.IndentedJSON(200, VSSRecordingResponse{
						Success: true,
						Message: "No active recording found for stream",
					})
					requestLogger.Warnln("No active recording found")
					return
				}
			}
			message = "Recording stopped and temporary files cleaned up."
		}
	}

	response := VSSRecordingResponse{
		Success: true,
		Message: message,
		Session: session,
	}

	if newSession != nil {
		response.NewSession = newSession
	}

	c.IndentedJSON(200, response)

	requestLogger.WithFields(logrus.Fields{
		"session_id":         func() string { if session != nil { return session.ID } else { return "none" } }(),
		"has_incident":       hasIncidentDetails,
		"is_start_next":      payload.IsStartNext,
		"new_session_id":     func() string { if newSession != nil { return newSession.ID } else { return "none" } }(),
		"stop_duration":      func() string { if session != nil { return session.StopDuration.String() } else { return "n/a" } }(),
		"start_duration":     func() string { if newSession != nil { return newSession.StartDuration.String() } else { return "n/a" } }(),
	}).Infoln("Recording stopped successfully")
}

// HTTPAPIServerDeleteRecording handles deleting recording files when no incident was created
func HTTPAPIServerDeleteRecording(c *gin.Context) {
	streamID := c.Param("uuid")
	channelID := c.Param("channel")

	requestLogger := log.WithFields(logrus.Fields{
		"module":  "http_recording",
		"stream":  streamID,
		"channel": channelID,
		"func":    "HTTPAPIServerDeleteRecording",
	})

	requestLogger.Infoln("Delete previous recording files request")

	// Get the current recording session to find the previous files
	session, err := recordingManager.GetRecordingStatus(streamID, channelID)
	if err != nil {
		c.IndentedJSON(404, VSSRecordingResponse{
			Success: false,
			Message: "No active recording session found for this stream/channel",
			Error:   err.Error(),
		})
		requestLogger.WithFields(logrus.Fields{"call": "GetRecordingStatus"}).Errorln(err.Error())
		return
	}

	// Queue the file deletion in background
	go func() {
		if err := recordingManager.DeletePreviousRecordingFiles(session.ID); err != nil {
			requestLogger.WithFields(logrus.Fields{
				"session_id": session.ID,
				"error":      err.Error(),
			}).Errorln("Failed to delete previous recording files")
		} else {
			requestLogger.WithFields(logrus.Fields{
				"session_id": session.ID,
			}).Infoln("Previous recording files deleted successfully")
		}
	}()

	c.IndentedJSON(200, VSSRecordingResponse{
		Success: true,
		Message: "Previous recording files deletion queued for background processing",
	})

	requestLogger.Infoln("Delete request queued successfully")
}

// HTTPAPIServerRemoveRecording completely stops recording and removes stream
func HTTPAPIServerRemoveRecording(c *gin.Context) {
	requestLogger := log.WithFields(logrus.Fields{
		"module":  "http_recording",
		"stream":  c.Param("uuid"),
		"channel": c.Param("channel"),
		"func":    "HTTPAPIServerRemoveRecording",
	})

	err := recordingManager.RemoveRecording(c.Param("uuid"), c.Param("channel"))
	if err != nil {
		c.IndentedJSON(400, VSSRecordingResponse{
			Success: false,
			Message: "Failed to remove recording",
			Error:   err.Error(),
		})
		requestLogger.WithFields(logrus.Fields{
			"call": "RemoveRecording",
		}).Errorln(err.Error())
		return
	}

	c.IndentedJSON(200, VSSRecordingResponse{
		Success: true,
		Message: "Recording completely stopped and stream/channel removed from configuration.",
	})

	requestLogger.WithFields(logrus.Fields{
		"stream":  c.Param("uuid"),
		"channel": c.Param("channel"),
	}).Infoln("Recording removed via API")
}

// HTTPAPIServerRecordingStatus gets the status of a recording session
func HTTPAPIServerRecordingStatus(c *gin.Context) {
	requestLogger := log.WithFields(logrus.Fields{
		"module":  "http_recording",
		"stream":  c.Param("uuid"),
		"channel": c.Param("channel"),
		"func":    "HTTPAPIServerRecordingStatus",
	})

	session, err := recordingManager.GetRecordingStatus(c.Param("uuid"), c.Param("channel"))
	if err != nil {
		c.IndentedJSON(404, VSSRecordingResponse{
			Success: false,
			Message: "Recording session not found",
			Error:   err.Error(),
		})
		requestLogger.WithFields(logrus.Fields{
			"call": "GetRecordingStatus",
		}).Errorln(err.Error())
		return
	}

	c.IndentedJSON(200, VSSRecordingResponse{
		Success: true,
		Message: "Recording status retrieved",
		Session: session,
	})

	requestLogger.WithFields(logrus.Fields{
		"session_id":     session.ID,
		"is_active":      session.Status == "recording",
		"fps":            session.FPS,
		"codec":          session.Codec,
		"start_duration": session.StartDuration.String(),
		"stop_duration":  session.StopDuration.String(),
	}).Debugln("Recording status retrieved successfully")
}
