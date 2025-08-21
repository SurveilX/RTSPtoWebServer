package main

import (
	"fmt"
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
		"session_id": session.ID,
		"method":     timing.Method,
		"rtsp_url":   payload.RTSPURL,
		"fps":        session.FPS,
		"codec":      session.Codec,
	}).Infoln("Recording started via API (no incident details yet)")
}

// HTTPAPIServerStopRecording stops current recording and organizes with incident details
func HTTPAPIServerStopRecording(c *gin.Context) {
	requestLogger := log.WithFields(logrus.Fields{
		"module":  "http_recording",
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

	hasIncidentDetails := payload.IncidentID != "" && payload.StoreID != "" && 
		payload.IncidentType != "" && payload.CallbackURL != ""

	if hasIncidentDetails {
		session, newSession, err := recordingManager.StopRecordingWithNext(
			c.Param("uuid"), 
			c.Param("channel"), 
			payload.IncidentID,
			payload.StoreID,
			payload.IncidentType,
			payload.CallbackURL,
			payload.IsStartNext,
		)
		if err != nil {
			c.IndentedJSON(400, VSSRecordingResponse{
				Success: false,
				Message: "Failed to stop recording with incident details",
				Error:   err.Error(),
			})
			requestLogger.WithFields(logrus.Fields{
				"call": "StopRecordingWithNext",
			}).Errorln(err.Error())
			return
		}

		message := fmt.Sprintf("Recording stopped and organized into VSS structure (/vss/alerts/%s/%s/). Video and poster uploaded to Digital Ocean Spaces and callback sent to %s with incidentId: %s", 
			convertIncidentType(payload.IncidentType), payload.StoreID, payload.CallbackURL, payload.IncidentID)
		
		if payload.IsStartNext && newSession != nil {
			message += fmt.Sprintf(". New recording started automatically (session: %s)", newSession.ID)
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
			"session_id":    session.ID,
			"incident_id":   payload.IncidentID,
			"callback_url":  payload.CallbackURL,
			"store_id":      payload.StoreID,
			"incident_type": payload.IncidentType,
			"is_start_next": payload.IsStartNext,
			"new_session_id": func() string {
				if newSession != nil {
					return newSession.ID
				}
				return ""
			}(),
		}).Infoln("Recording stopped with incident details - VSS organization and upload scheduled")
	} else {
		session, newSession, err := recordingManager.StopRecordingNoIncidentWithNext(
			c.Param("uuid"), 
			c.Param("channel"),
			payload.IsStartNext,
		)

		if err != nil {
			c.IndentedJSON(400, VSSRecordingResponse{
				Success: false,
				Message: "Failed to stop recording",
				Error:   err.Error(),
			})
			requestLogger.WithFields(logrus.Fields{
				"call": "StopRecordingNoIncidentWithNext",
			}).Errorln(err.Error())
			return
		}

		message := "Recording stopped and temporary files cleaned up. No incident generated."
		if payload.IsStartNext && newSession != nil {
			message += fmt.Sprintf(" New recording started automatically (session: %s)", newSession.ID)
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
			"session_id": session.ID,
			"is_start_next": payload.IsStartNext,
			"new_session_id": func() string {
				if newSession != nil {
					return newSession.ID
				}
				return ""
			}(),
		}).Infoln("Recording stopped without incident - temporary files cleaned up")
	}
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
		"session_id": session.ID,
		"is_active":  session.Status == "recording",
		"fps":        session.FPS,
		"codec":      session.Codec,
	}).Debugln("Recording status retrieved successfully")
}
