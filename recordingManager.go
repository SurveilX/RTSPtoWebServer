package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/deepch/vdk/av"
	"github.com/deepch/vdk/format/mp4"
	"github.com/sirupsen/logrus"
)

// RecordingSession represents an active recording session
type RecordingSession struct {
	ID           string    `json:"id"`
	StreamID     string    `json:"stream_id"`
	ChannelID    string    `json:"channel_id"`
	StartTime    time.Time `json:"start_time"`
	Status       string    `json:"status"` // "recording", "stopping", "stopped", "uploading", "failed"
	FilePath     string    `json:"file_path"`
	PosterPath   string    `json:"poster_path"`
	UseFFmpeg    bool      `json:"use_ffmpeg"`
	RTSPURL      string    `json:"rtsp_url"`
	Continuous   bool      `json:"continuous"`

	FPS          int    `json:"fps,omitempty"`
	Codec        string `json:"codec,omitempty"`

	StartDuration time.Duration `json:"start_duration,omitempty"`
	StopDuration  time.Duration `json:"stop_duration,omitempty"`

	IncidentType string `json:"incident_type"`
	StoreID      string `json:"store_id"`
	IncidentID   string `json:"incident_id"`
	CallbackURL  string `json:"callback_url"`

	ctx         context.Context
	cancel      context.CancelFunc
	muxer       *mp4.Muxer
	file        *os.File
	ffmpegCmd   *exec.Cmd
	clientID    string
	mutex       sync.RWMutex

	lastTimestamp time.Duration
	baseTimestamp time.Duration
	timestampSet  bool
}

// PendingUpload represents a file waiting to be uploaded
type PendingUpload struct {
	SessionID    string    `json:"session_id"`
	FilePath     string    `json:"file_path"`
	PosterPath   string    `json:"poster_path"`
	IncidentID   string    `json:"incident_id"`
	IncidentType string    `json:"incident_type"`
	StoreID      string    `json:"store_id"`
	CallbackURL  string    `json:"callback_url"`
	CreatedAt    time.Time `json:"created_at"`
}

// RecordingManager manages all recording sessions
type RecordingManager struct {
	sessions map[string]*RecordingSession
	mutex    sync.RWMutex

	uploadQueue chan *PendingUpload

	stopStartActive   bool
	stopStartMutex    sync.RWMutex
	stopStartWaitChan chan struct{}

	autoStopTicker *time.Ticker
	stopChecker    chan bool

	cleanupTicker *time.Ticker
	cleanupStopper chan bool
}

var recordingManager *RecordingManager

func init() {
	recordingManager = &RecordingManager{
		sessions:          make(map[string]*RecordingSession),
		uploadQueue:       make(chan *PendingUpload, 100),
		stopChecker:       make(chan bool),
		cleanupStopper:    make(chan bool),
		stopStartWaitChan: make(chan struct{}, 1),
	}
	
	recordingManager.startAutoStopChecker()
	recordingManager.startCleanupWorker()
	recordingManager.startUploadWorker()
}

// setStopStartActive sets the stop-start operation status
func (rm *RecordingManager) setStopStartActive(active bool) {
	log.WithFields(logrus.Fields{
		"module": "recording",
		"active": active,
		"step":   "priority_change_start",
	}).Debugln("setStopStartActive: Acquiring stopStartMutex")
	
	rm.stopStartMutex.Lock()
	defer rm.stopStartMutex.Unlock()
	
	log.WithFields(logrus.Fields{
		"module": "recording",
		"active": active,
		"step":   "priority_change_mutex_acquired",
	}).Debugln("setStopStartActive: stopStartMutex acquired")
	
	if active {
		rm.stopStartActive = true
		log.WithFields(logrus.Fields{
			"module": "recording",
			"step":   "priority_set_active",
		}).Infoln("StopAndStart operations now have priority")
	} else {
		rm.stopStartActive = false
		select {
		case rm.stopStartWaitChan <- struct{}{}:
			log.WithFields(logrus.Fields{
				"module": "recording",
				"step":   "priority_signal_sent",
			}).Debugln("setStopStartActive: Signal sent to waiting workers")
		default:
			log.WithFields(logrus.Fields{
				"module": "recording",
				"step":   "priority_signal_not_needed",
			}).Debugln("setStopStartActive: No workers waiting for signal")
		}
		log.WithFields(logrus.Fields{
			"module": "recording",
			"step":   "priority_cleared",
		}).Infoln("StopAndStart operations completed, upload queue can proceed")
	}
}

// isStopStartActive checks if stop-start operations are active
func (rm *RecordingManager) isStopStartActive() bool {
	rm.stopStartMutex.RLock()
	defer rm.stopStartMutex.RUnlock()
	return rm.stopStartActive
}

// waitForStopStartCompletion waits for stop-start operations to complete
func (rm *RecordingManager) waitForStopStartCompletion() {
	if !rm.isStopStartActive() {
		return
	}
	
	log.WithFields(logrus.Fields{
		"module": "upload_worker",
	}).Debugln("Waiting for StopAndStart operations to complete")
	
	<-rm.stopStartWaitChan
	
	log.WithFields(logrus.Fields{
		"module": "upload_worker",
	}).Debugln("StopAndStart operations completed, proceeding with upload")
}

// startUploadWorker processes uploads in the background with priority control
func (rm *RecordingManager) startUploadWorker() {
	go func() {
		for upload := range rm.uploadQueue {
			rm.waitForStopStartCompletion()

			log.WithFields(logrus.Fields{
				"module":        "upload_worker",
				"session_id":    upload.SessionID,
				"incident_id":   upload.IncidentID,
				"incident_type": upload.IncidentType,
				"store_id":      upload.StoreID,
			}).Infoln("Processing upload from queue")
			
			uploadStart := time.Now()

			tempSession := &RecordingSession{
				ID:           upload.SessionID,
				FilePath:     upload.FilePath,
				PosterPath:   upload.PosterPath,
				IncidentID:   upload.IncidentID,
				IncidentType: upload.IncidentType,
				StoreID:      upload.StoreID,
				CallbackURL:  upload.CallbackURL,
			}
			
			if err := rm.processVSSRecording(tempSession); err != nil {
				log.WithFields(logrus.Fields{
					"module":        "upload_worker",
					"session_id":    upload.SessionID,
					"error":         err.Error(),
					"upload_duration": time.Since(uploadStart).String(),
				}).Errorln("Failed to process VSS recording from queue")
			} else {
				log.WithFields(logrus.Fields{
					"module":        "upload_worker",
					"session_id":    upload.SessionID,
					"upload_duration": time.Since(uploadStart).String(),
				}).Infoln("Successfully processed upload from queue")
			}
		}
	}()
	
	log.WithFields(logrus.Fields{
		"module": "upload_worker",
	}).Infoln("Upload worker started")
}

func (rm *RecordingManager) startAutoStopChecker() {
	rm.autoStopTicker = time.NewTicker(45 * time.Second)
	
	go func() {
		for {
			select {
			case <-rm.autoStopTicker.C:
				rm.checkAndStopLongRunningSessions()
			case <-rm.stopChecker:
				rm.autoStopTicker.Stop()
				return
			}
		}
	}()
	
	log.WithFields(logrus.Fields{
		"module":        "recording",
		"check_interval": "45s",
		"max_duration":   "1m30s",
	}).Infoln("Auto-stop checker started")
}

// checkAndStopLongRunningSessions checks for sessions running longer than 1m30s
func (rm *RecordingManager) checkAndStopLongRunningSessions() {
	rm.mutex.RLock()
	var sessionsToStop []*RecordingSession
	
	maxDuration := 90 * time.Second
	
	for _, session := range rm.sessions {
		session.mutex.RLock()
		if session.Status == "recording" && time.Since(session.StartTime) > maxDuration {
			sessionsToStop = append(sessionsToStop, session)
		}
		session.mutex.RUnlock()
	}
	rm.mutex.RUnlock()

	for _, session := range sessionsToStop {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"stream":     session.StreamID,
			"channel":    session.ChannelID,
			"duration":   time.Since(session.StartTime).String(),
			"max_duration": maxDuration.String(),
		}).Warnln("Auto-stopping long-running recording session")

		go func(s *RecordingSession) {
			_, _, err := rm.StopRecordingNoIncidentWithNext(s.StreamID, s.ChannelID, false)
			if err != nil {
				log.WithFields(logrus.Fields{
					"module":     "recording",
					"session_id": s.ID,
					"error":      err.Error(),
				}).Errorln("Failed to auto-stop long-running recording")
			}
		}(session)
	}
	
	if len(sessionsToStop) > 0 {
		log.WithFields(logrus.Fields{
			"module":         "recording",
			"stopped_count":  len(sessionsToStop),
			"check_interval": "45s",
		}).Infoln("Auto-stop check completed")
	}
}

func (rm *RecordingManager) stopAutoStopChecker() {
	close(rm.stopChecker)
}

// startCleanupWorker starts a worker that runs every 5 minutes to clean up old files
func (rm *RecordingManager) startCleanupWorker() {
	rm.cleanupTicker = time.NewTicker(5 * time.Minute)
	
	go func() {
		for {
			select {
			case <-rm.cleanupTicker.C:
				rm.waitForStopStartCompletion()
				rm.cleanupOldFiles()
			case <-rm.cleanupStopper:
				rm.cleanupTicker.Stop()
				return
			}
		}
	}()
	
	log.WithFields(logrus.Fields{
		"module":        "cleanup",
		"check_interval": "5m",
		"max_age":       "2m",
	}).Infoln("Cleanup worker started")
}

// cleanupOldFiles removes files older than 2 minutes and empty directories
func (rm *RecordingManager) cleanupOldFiles() {
	now := time.Now()
	maxAge := 2 * time.Minute

	tempDir := "temp/recordings"
	if _, err := os.Stat(tempDir); err == nil {
		rm.cleanupDirectory(tempDir, now, maxAge)
	}

	vssDir := "vss"
	if _, err := os.Stat(vssDir); err == nil {
		rm.cleanupDirectory(vssDir, now, 10*time.Minute)
	}
}

// cleanupDirectory recursively cleans up files and empty directories
func (rm *RecordingManager) cleanupDirectory(dirPath string, now time.Time, maxAge time.Duration) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		log.WithFields(logrus.Fields{
			"module": "cleanup",
			"dir":    dirPath,
			"error":  err.Error(),
		}).Warnln("Failed to read directory for cleanup")
		return
	}
	
	filesDeleted := 0
	dirsDeleted := 0
	
	for _, entry := range entries {
		fullPath := filepath.Join(dirPath, entry.Name())
		
		if entry.IsDir() {
			rm.cleanupDirectory(fullPath, now, maxAge)

			if rm.removeEmptyDirectory(fullPath) {
				dirsDeleted++
			}
		} else {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			
			fileAge := now.Sub(info.ModTime())
			if fileAge > maxAge {
				if rm.isFileInActiveSession(fullPath) {
					log.WithFields(logrus.Fields{
						"module":   "cleanup",
						"file":     fullPath,
						"age":      fileAge.String(),
					}).Debugln("Skipping cleanup of active session file")
					continue
				}
				
				if err := os.Remove(fullPath); err != nil {
					log.WithFields(logrus.Fields{
						"module": "cleanup",
						"file":   fullPath,
						"age":    fileAge.String(),
						"error":  err.Error(),
					}).Warnln("Failed to delete old file")
				} else {
					filesDeleted++
					log.WithFields(logrus.Fields{
						"module": "cleanup",
						"file":   fullPath,
						"age":    fileAge.String(),
					}).Infoln("Deleted old file")
				}
			}
		}
	}
	
	if filesDeleted > 0 || dirsDeleted > 0 {
		log.WithFields(logrus.Fields{
			"module":        "cleanup",
			"directory":     dirPath,
			"files_deleted": filesDeleted,
			"dirs_deleted":  dirsDeleted,
		}).Infoln("Cleanup completed for directory")
	}
}

// removeEmptyDirectory removes a directory if it's empty
func (rm *RecordingManager) removeEmptyDirectory(dirPath string) bool {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return false
	}
	
	if len(entries) == 0 {
		if err := os.Remove(dirPath); err != nil {
			log.WithFields(logrus.Fields{
				"module": "cleanup",
				"dir":    dirPath,
				"error":  err.Error(),
			}).Debugln("Failed to remove empty directory")
			return false
		}
		
		log.WithFields(logrus.Fields{
			"module": "cleanup",
			"dir":    dirPath,
		}).Infoln("Removed empty directory")
		return true
	}
	
	return false
}

// isFileInActiveSession checks if a file is part of an active recording session
func (rm *RecordingManager) isFileInActiveSession(filePath string) bool {
	rm.mutex.RLock()
	defer rm.mutex.RUnlock()
	
	for _, session := range rm.sessions {
		session.mutex.RLock()
		isActive := session.Status == "recording" || session.Status == "stopping"
		matchesVideo := session.FilePath == filePath
		matchesPoster := session.PosterPath == filePath
		session.mutex.RUnlock()
		
		if isActive && (matchesVideo || matchesPoster) {
			return true
		}
	}
	
	return false
}

// stopCleanupWorker stops the cleanup worker
func (rm *RecordingManager) stopCleanupWorker() {
	close(rm.cleanupStopper)
}

// convertIncidentType converts "Phone Usage" to "phone-usage"
func convertIncidentType(incidentType string) string {
	converted := strings.ToLower(incidentType)
	converted = strings.ReplaceAll(converted, " ", "-")

	converted = strings.ReplaceAll(converted, "_", "-")
	return converted
}

// generateShortUUID generates a short 8-character UUID
func generateShortUUID() string {
	bytes := make([]byte, 4)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// generateVSSFilePaths generates VSS-compliant file paths
func generateVSSFilePaths(incidentType, storeID string) (string, string, error) {
	currentTime := time.Now()
	shortUUID := generateShortUUID()
	convertedIncidentType := convertIncidentType(incidentType)
	
	// path: /vss/alerts/{incidentType}/{storeId}/{current_time.strftime('%Y%m%d_%H%M%S')}{uuid.uuid4().hex[:8]}.mp4
	videoFileName := fmt.Sprintf("%s%s.mp4", 
		currentTime.Format("20060102_150405"), 
		shortUUID)
	videoPath := filepath.Join("vss", "alerts", convertedIncidentType, storeID, videoFileName)
	
	// path: /vss/posters/{incidentType}/{storeId}/{current_time.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.jpg
	posterFileName := fmt.Sprintf("%s_%s.jpg", 
		currentTime.Format("20060102150405"), 
		shortUUID)
	posterPath := filepath.Join("vss", "posters", convertedIncidentType, storeID, posterFileName)
	
	return videoPath, posterPath, nil
}

// generateTemporaryFilePaths generates temporary file paths for recording
func generateTemporaryFilePaths(sessionID string) (string, string, error) {
	currentTime := time.Now()
	
	// temp path: temp/recordings/{sessionID}/{timestamp}.mp4
	videoFileName := fmt.Sprintf("%s.mp4", currentTime.Format("20060102_150405"))
	videoPath := filepath.Join("temp", "recordings", sessionID, videoFileName)
	
	// temp path: temp/recordings/{sessionID}/{timestamp}.jpg
	posterFileName := fmt.Sprintf("%s.jpg", currentTime.Format("20060102_150405"))
	posterPath := filepath.Join("temp", "recordings", sessionID, posterFileName)
	
	return videoPath, posterPath, nil
}

// organizeVSSFiles moves files from temporary location to VSS structure
func (rm *RecordingManager) organizeVSSFiles(session *RecordingSession, incidentType, storeID string) error {
	vssVideoPath, vssPosterPath, err := generateVSSFilePaths(incidentType, storeID)
	if err != nil {
		return fmt.Errorf("failed to generate VSS paths: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(vssVideoPath), 0755); err != nil {
		return fmt.Errorf("failed to create VSS video directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(vssPosterPath), 0755); err != nil {
		return fmt.Errorf("failed to create VSS poster directory: %v", err)
	}

	if _, err := os.Stat(session.FilePath); err == nil {
		if err := os.Rename(session.FilePath, vssVideoPath); err != nil {
			return fmt.Errorf("failed to move video file to VSS location: %v", err)
		}
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"from":       session.FilePath,
			"to":         vssVideoPath,
		}).Infoln("Moved video file to VSS location")
		session.FilePath = vssVideoPath
	}

	if _, err := os.Stat(session.PosterPath); err == nil {
		if err := os.Rename(session.PosterPath, vssPosterPath); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"error":      err.Error(),
			}).Warnln("Failed to move poster file to VSS location")
		} else {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"from":       session.PosterPath,
				"to":         vssPosterPath,
			}).Infoln("Moved poster file to VSS location")
		}
	} else {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"old_path":   session.PosterPath,
			"new_path":   vssPosterPath,
		}).Infoln("Poster file doesn't exist yet, updating path for future generation")
	}

	session.PosterPath = vssPosterPath

	session.IncidentType = incidentType
	session.StoreID = storeID

	return nil
}

// startRecordingInternal starts a new recording session (assumes mutex is already held)
func (rm *RecordingManager) startRecordingInternal(streamID, channelID, rtspURL string, useFFmpeg bool, incidentType, storeID string, fps int, codec string) (*RecordingSession, error) {
	startTime := time.Now()

	log.WithFields(logrus.Fields{
		"module":        "recording",
		"stream_id":     streamID,
		"channel_id":    channelID,
		"rtsp_url":      rtspURL,
		"use_ffmpeg":    useFFmpeg,
		"incident_type": incidentType,
		"store_id":      storeID,
		"fps":           fps,
		"codec":         codec,
		"step":          "start_recording_internal_entry",
	}).Infoln("startRecordingInternal: Function entry (mutex already held)")

	if !Storage.StreamChannelExist(streamID, channelID) {
		log.WithFields(logrus.Fields{
			"module":  "recording",
			"stream":  streamID,
			"channel": channelID,
		}).Infoln("Stream/channel not found, creating automatically")

		if !Storage.StreamExist(streamID) {
			streamConfig := StreamST{
				Name:     fmt.Sprintf("Recording Stream %s", streamID),
				Channels: make(map[string]ChannelST),
			}
			if err := Storage.StreamAdd(streamID, streamConfig); err != nil {
				return nil, fmt.Errorf("failed to create stream: %v", err)
			}
			
			log.WithFields(logrus.Fields{
				"module":  "recording",
				"stream":  streamID,
			}).Infoln("Created new stream for recording")
		}

		channelConfig := ChannelST{
			Name:     fmt.Sprintf("Recording Channel %s", channelID),
			URL:      rtspURL,
			OnDemand: false,
			Debug:    false,
			Audio:    false,
		}

		if err := Storage.StreamChannelAdd(streamID, channelID, channelConfig); err != nil {
			return nil, fmt.Errorf("failed to create channel: %v", err)
		}
	}

	sessionKey := fmt.Sprintf("%s_%s", streamID, channelID)
	if session, exists := rm.sessions[sessionKey]; exists && (session.Status == "recording" || session.Status == "failed") {
		if session.Status == "failed" {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"stream":     streamID,
				"channel":    channelID,
			}).Infoln("Cleaning up failed session before starting new recording")

			if session.cancel != nil {
				session.cancel()
			}

			delete(rm.sessions, sessionKey)
		} else {
			return nil, fmt.Errorf("recording already in progress for stream %s channel %s", streamID, channelID)
		}
	}

	sessionID, err := generateUUID()
	if err != nil {
		return nil, err
	}

	var videoPath, posterPath string

	if incidentType != "" && storeID != "" {
		videoPath, posterPath, err = generateVSSFilePaths(incidentType, storeID)
		if err != nil {
			return nil, fmt.Errorf("failed to generate VSS file paths: %v", err)
		}
	} else {
		videoPath, posterPath, err = generateTemporaryFilePaths(sessionID)
		if err != nil {
			return nil, fmt.Errorf("failed to generate temporary file paths: %v", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(videoPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create video directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(posterPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create poster directory: %v", err)
	}

	if fps <= 0 {
		fps = 30
	}
	if codec == "" {
		codec = "h264"
	}

	ctx, cancel := context.WithCancel(context.Background())
	session := &RecordingSession{
		ID:           sessionID,
		StreamID:     streamID,
		ChannelID:    channelID,
		StartTime:    time.Now(),
		Status:       "recording",
		FilePath:     videoPath,
		PosterPath:   posterPath,
		UseFFmpeg:    useFFmpeg,
		RTSPURL:      rtspURL,
		Continuous:   true,
		FPS:          fps,
		Codec:        codec,
		IncidentType: incidentType,
		StoreID:      storeID,
		ctx:          ctx,
		cancel:       cancel,
		timestampSet: false,
	}

	if useFFmpeg {
		err = rm.startFFmpegRecording(session)
	} else {
		err = rm.startNativeRecording(session)
	}

	if err != nil {
		cancel()
		return nil, err
	}

	rm.sessions[sessionKey] = session

	startDuration := time.Since(startTime)
	session.StartDuration = startDuration

	log.WithFields(logrus.Fields{
		"module":         "recording",
		"session_id":     sessionID,
		"stream":         streamID,
		"channel":        channelID,
		"fps":            fps,
		"codec":          codec,
		"method":         map[bool]string{true: "ffmpeg", false: "native"}[useFFmpeg],
		"incident_type":  incidentType,
		"store_id":       storeID,
		"video_path":     videoPath,
		"poster_path":    posterPath,
		"temporary":      incidentType == "" || storeID == "",
		"auto_stop":      "45s",
		"max_duration":   "1m30s",
		"start_duration": startDuration.String(),
		"step":           "start_recording_internal_success",
	}).Infoln("startRecordingInternal: Recording started successfully")

	return session, nil
}

// StartRecording starts a new recording session
func (rm *RecordingManager) StartRecording(streamID, channelID, rtspURL string, useFFmpeg bool, incidentType, storeID string, fps int, codec string) (*RecordingSession, error) {
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"step":       "start_recording_acquire_mutex",
	}).Infoln("StartRecording: Acquiring main mutex")

	rm.mutex.Lock()
	defer func() {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"stream_id":  streamID,
			"channel_id": channelID,
			"step":       "start_recording_release_mutex",
		}).Infoln("StartRecording: Releasing main mutex")
		rm.mutex.Unlock()
	}()

	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"step":       "start_recording_mutex_acquired",
	}).Infoln("StartRecording: Main mutex acquired")

	return rm.startRecordingInternal(streamID, channelID, rtspURL, useFFmpeg, incidentType, storeID, fps, codec)
}

// startFFmpegRecording starts recording using FFmpeg with VSS paths
func (rm *RecordingManager) startFFmpegRecording(session *RecordingSession) error {
	args := []string{
		"-i", session.RTSPURL,
		"-an", // Disable audio
		"-b:v", "900k", // Specify video bitrate
		"-vcodec", "copy",
		"-avoid_negative_ts", "make_zero", // Fix timestamp issues
		"-fflags", "+genpts", // Generate presentation timestamps
		"-r", "30", // Reduced frame rate for stability
		"-y",
		session.FilePath,
	}

	session.ffmpegCmd = exec.CommandContext(session.ctx, "ffmpeg", args...)

	session.ffmpegCmd.Stderr = &bytes.Buffer{}

	if err := session.ffmpegCmd.Start(); err != nil {
		return fmt.Errorf("failed to start ffmpeg: %v", err)
	}

	go func() {
		err := session.ffmpegCmd.Wait()
		session.mutex.Lock()
		if session.Status == "recording" {
			if err != nil && session.ctx.Err() == nil {
				log.WithFields(logrus.Fields{
					"module":     "recording",
					"session_id": session.ID,
					"error":      err.Error(),
					"stderr":     session.ffmpegCmd.Stderr.(*bytes.Buffer).String(),
				}).Errorln("FFmpeg recording failed")
				session.Status = "failed"
			} else {
				session.Status = "stopped"
			}
		}
		session.mutex.Unlock()
	}()

	return nil
}

// normalizePacketTimestamp fixes timestamp issues for MP4 muxer
func (session *RecordingSession) normalizePacketTimestamp(packet *av.Packet) *av.Packet {
	normalizedPacket := *packet
	
	if !session.timestampSet {
		session.baseTimestamp = packet.Time
		session.lastTimestamp = 0
		session.timestampSet = true
		normalizedPacket.Time = 0
	} else {
		relativeTime := packet.Time - session.baseTimestamp

		if relativeTime <= session.lastTimestamp {
			frameDuration := time.Second / time.Duration(session.FPS)
			if frameDuration == 0 {
				frameDuration = time.Millisecond * 33
			}
			relativeTime = session.lastTimestamp + frameDuration
		}
		
		session.lastTimestamp = relativeTime
		normalizedPacket.Time = relativeTime
	}
	
	return &normalizedPacket
}

// startNativeRecording starts recording using native Go MP4 muxer with VSS paths
func (rm *RecordingManager) startNativeRecording(session *RecordingSession) error {
	file, err := os.Create(session.FilePath)
	if err != nil {
		return fmt.Errorf("failed to create recording file: %v", err)
	}
	session.file = file

	Storage.StreamChannelRun(session.StreamID, session.ChannelID)

	var allCodecs []av.CodecData
	for i := 0; i < 60; i++ {
		allCodecs, err = Storage.StreamChannelCodecs(session.StreamID, session.ChannelID)
		if err == nil && len(allCodecs) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err != nil {
		file.Close()
		return fmt.Errorf("failed to get stream codecs: %v", err)
	}

	var videoCodecs []av.CodecData
	for _, codec := range allCodecs {
		if codec.Type().IsVideo() {
			videoCodecs = append(videoCodecs, codec)
		}
	}

	if len(videoCodecs) == 0 {
		file.Close()
		return fmt.Errorf("no video codecs found in stream")
	}

	session.muxer = mp4.NewMuxer(file)
	if err := session.muxer.WriteHeader(videoCodecs); err != nil {
		file.Close()
		return fmt.Errorf("failed to write MP4 header: %v", err)
	}

	clientID, ch, _, err := Storage.ClientAdd(session.StreamID, session.ChannelID, MSE)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to add recording client: %v", err)
	}
	session.clientID = clientID

	go func() {
		defer func() {
			log.WithFields(logrus.Fields{
				"module":       "recording",
				"session_id":   session.ID,
			}).Infoln("Native recording cleanup started")
			
			if session.muxer != nil {
				if err := session.muxer.WriteTrailer(); err != nil {
					log.WithFields(logrus.Fields{
						"module":     "recording",
						"session_id": session.ID,
						"error":      err.Error(),
					}).Errorln("Failed to write MP4 trailer")
				} else {
					log.WithFields(logrus.Fields{
						"module":     "recording",
						"session_id": session.ID,
					}).Infoln("MP4 trailer written successfully")
				}
			}
			
			if session.file != nil {
				if err := session.file.Sync(); err != nil {
					log.WithFields(logrus.Fields{
						"module":     "recording",
						"session_id": session.ID,
						"error":      err.Error(),
					}).Warnln("Failed to sync file")
				}
				
				if err := session.file.Close(); err != nil {
					log.WithFields(logrus.Fields{
						"module":     "recording",
						"session_id": session.ID,
						"error":      err.Error(),
					}).Errorln("Failed to close file")
				} else {
					log.WithFields(logrus.Fields{
						"module":     "recording",
						"session_id": session.ID,
					}).Infoln("Recording file closed successfully")
				}
			}
			
			Storage.ClientDelete(session.StreamID, session.clientID, session.ChannelID)
			
			log.WithFields(logrus.Fields{
				"module":       "recording",
				"session_id":   session.ID,
			}).Infoln("Native recording cleanup completed")
		}()

		var videoStart bool
		noVideo := time.NewTimer(30 * time.Second)
		defer noVideo.Stop()
		
		packetCount := 0

		for {
			select {
			case <-session.ctx.Done():
				session.mutex.Lock()
				if session.Status == "recording" {
					session.Status = "stopped"
				}
				session.mutex.Unlock()
				
				log.WithFields(logrus.Fields{
					"module":       "recording",
					"session_id":   session.ID,
					"packet_count": packetCount,
				}).Infoln("Recording stopped by context, allowing cleanup")

				time.Sleep(100 * time.Millisecond)
				return
			case <-noVideo.C:
				session.mutex.Lock()
				if session.Status == "recording" {
					session.Status = "failed"
					log.WithFields(logrus.Fields{
						"module":       "recording",
						"session_id":   session.ID,
						"packet_count": packetCount,
					}).Errorln("Recording failed: no video data received within 30 seconds")
				}
				session.mutex.Unlock()
				return
			case packet := <-ch:
				if packet.Idx != 0 {
					continue
				}

				packetCount++

				if packet.IsKeyFrame {
					noVideo.Reset(30 * time.Second)
					videoStart = true
				}
				if !videoStart {
					continue
				}

				normalizedPacket := session.normalizePacketTimestamp(packet)

				if err := session.muxer.WritePacket(*normalizedPacket); err != nil {
					session.mutex.Lock()
					if session.Status == "recording" {
						session.Status = "failed"
						log.WithFields(logrus.Fields{
							"module":       "recording",
							"session_id":   session.ID,
							"error":        err.Error(),
							"packet_count": packetCount,
							"packet_time":  normalizedPacket.Time.String(),
							"last_time":    session.lastTimestamp.String(),
						}).Errorln("Recording failed: write packet error with timestamp details")
					}
					session.mutex.Unlock()
					return
				}

				if packetCount%100 == 0 {
					log.WithFields(logrus.Fields{
						"module":       "recording",
						"session_id":   session.ID,
						"packet_count": packetCount,
						"duration":     time.Since(session.StartTime).String(),
						"packet_time":  normalizedPacket.Time.String(),
					}).Debugln("Recording progress")
				}
			}
		}
	}()

	return nil
}

// StopAndStart stops current recording immediately and starts next recording
func (rm *RecordingManager) StopAndStart(streamID, channelID string) (*RecordingSession, *RecordingSession, error) {
	stopStartTime := time.Now()

	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"action":     "stop_and_start",
		"step":       "1_entry",
	}).Infoln("StopAndStart: Function entry")

	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"step":       "2_set_priority",
	}).Infoln("StopAndStart: Setting priority flag")
	
	rm.setStopStartActive(true)
	defer func() {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"stream_id":  streamID,
			"channel_id": channelID,
			"step":       "99_clear_priority",
		}).Infoln("StopAndStart: Clearing priority flag")
		rm.setStopStartActive(false)
	}()
	
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"step":       "3_acquire_mutex",
	}).Infoln("StopAndStart: Acquiring main mutex")
	
	rm.mutex.Lock()
	
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"step":       "4_mutex_acquired",
	}).Infoln("StopAndStart: Main mutex acquired")
	
	defer func() {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"stream_id":  streamID,
			"channel_id": channelID,
			"step":       "98_release_mutex",
		}).Infoln("StopAndStart: Releasing main mutex")
		rm.mutex.Unlock()
	}()

	sessionKey := fmt.Sprintf("%s_%s", streamID, channelID)
	
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_key": sessionKey,
		"step":       "5_check_session",
	}).Infoln("StopAndStart: Checking for existing session")

	session, exists := rm.sessions[sessionKey]
	if !exists {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"stream_id":  streamID,
			"channel_id": channelID,
			"step":       "6_no_session",
		}).Warnln("StopAndStart: No active recording found for stream, will start new recording")

		log.WithFields(logrus.Fields{
			"module":     "recording",
			"stream_id":  streamID,
			"channel_id": channelID,
			"step":       "7_start_new_no_prev",
		}).Infoln("StopAndStart: Starting new recording (no previous session)")

		newSession, err := rm.startRecordingInternal(streamID, channelID, "", false, "", "", 30, "h264")
		if err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"stream_id":  streamID,
				"channel_id": channelID,
				"step":       "8_start_error",
				"error":      err.Error(),
			}).Errorln("StopAndStart: Failed to start new recording")
			return nil, nil, fmt.Errorf("failed to start new recording: %v", err)
		}
		
		log.WithFields(logrus.Fields{
			"module":           "recording",
			"stream_id":        streamID,
			"channel_id":       channelID,
			"new_session_id":   newSession.ID,
			"stop_start_duration": time.Since(stopStartTime).String(),
			"step":             "9_success_no_prev",
		}).Infoln("StopAndStart: Started new recording (no previous session found)")
		
		return nil, newSession, nil
	}

	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "10_session_found",
	}).Infoln("StopAndStart: Found existing session")

	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "11_acquire_session_mutex",
	}).Infoln("StopAndStart: Acquiring session mutex")

	session.mutex.Lock()
	
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "12_session_mutex_acquired",
	}).Infoln("StopAndStart: Session mutex acquired")

	currentStatus := session.Status
	originalRTSPURL := session.RTSPURL
	originalUseFFmpeg := session.UseFFmpeg
	originalFPS := session.FPS
	originalCodec := session.Codec

	log.WithFields(logrus.Fields{
		"module":         "recording",
		"stream_id":      streamID,
		"channel_id":     channelID,
		"session_id":     session.ID,
		"current_status": currentStatus,
		"rtsp_url":       originalRTSPURL,
		"use_ffmpeg":     originalUseFFmpeg,
		"fps":            originalFPS,
		"codec":          originalCodec,
		"step":           "13_session_details",
	}).Infoln("StopAndStart: Retrieved session details")

	session.Status = "stopping"
	
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "14_status_set_stopping",
	}).Infoln("StopAndStart: Set status to stopping")
	
	session.mutex.Unlock()
	
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "15_session_mutex_released",
	}).Infoln("StopAndStart: Session mutex released")

	// Stop the current recording synchronously
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "16_cancel_context",
	}).Infoln("StopAndStart: Canceling recording context")
	
	session.cancel()
	
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "17_context_canceled",
	}).Infoln("StopAndStart: Recording context canceled")
	
	// Wait a brief moment for the recording to stop
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "18_wait_stop",
	}).Infoln("StopAndStart: Waiting 500ms for recording to stop")
	
	time.Sleep(500 * time.Millisecond)
	
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "19_wait_complete",
	}).Infoln("StopAndStart: Wait complete")
	
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "20_acquire_session_mutex_final",
	}).Infoln("StopAndStart: Acquiring session mutex for final status update")
	
	session.mutex.Lock()
	if session.Status == "stopping" {
		session.Status = "stopped"
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"stream_id":  streamID,
			"channel_id": channelID,
			"session_id": session.ID,
			"step":       "21_status_set_stopped",
		}).Infoln("StopAndStart: Set status to stopped")
	} else {
		log.WithFields(logrus.Fields{
			"module":       "recording",
			"stream_id":    streamID,
			"channel_id":   channelID,
			"session_id":   session.ID,
			"final_status": session.Status,
			"step":         "21_status_already_changed",
		}).Infoln("StopAndStart: Status already changed")
	}
	session.mutex.Unlock()
	
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "22_session_mutex_released_final",
	}).Infoln("StopAndStart: Session mutex released after final status update")

	log.WithFields(logrus.Fields{
		"module":     "recording",
		"session_id": session.ID,
		"stream":     streamID,
		"channel":    channelID,
		"step":       "23_previous_stopped",
	}).Infoln("StopAndStart: Previous recording stopped")

	// Remove from sessions map
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "24_remove_from_map",
	}).Infoln("StopAndStart: Removing session from map")
	
	delete(rm.sessions, sessionKey)
	
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"session_id": session.ID,
		"step":       "25_removed_from_map",
	}).Infoln("StopAndStart: Session removed from map")

	// Start new recording immediately
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"stream_id":  streamID,
		"channel_id": channelID,
		"step":       "26_start_new_recording",
	}).Infoln("StopAndStart: Starting new recording")
	
	newSession, err := rm.startRecordingInternal(streamID, channelID, originalRTSPURL, originalUseFFmpeg, "", "", originalFPS, originalCodec)
	if err != nil {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"stream_id":  streamID,
			"channel_id": channelID,
			"step":       "27_start_new_error",
			"error":      err.Error(),
		}).Errorln("StopAndStart: Failed to start new recording")
		return session, nil, fmt.Errorf("failed to start new recording: %v", err)
	}
	
	log.WithFields(logrus.Fields{
		"module":         "recording",
		"stream_id":      streamID,
		"channel_id":     channelID,
		"new_session_id": newSession.ID,
		"step":           "28_new_recording_started",
	}).Infoln("StopAndStart: New recording started successfully")

	stopStartDuration := time.Since(stopStartTime)
	session.StopDuration = stopStartDuration

	log.WithFields(logrus.Fields{
		"module":              "recording",
		"old_session_id":      session.ID,
		"new_session_id":      newSession.ID,
		"stream":              streamID,
		"channel":             channelID,
		"prev_status":         currentStatus,
		"stop_start_duration": stopStartDuration.String(),
		"step":                "29_final_success",
	}).Infoln("StopAndStart: Stop and start completed successfully")

	return session, newSession, nil
}

// QueueUpload queues a recording for background upload processing
func (rm *RecordingManager) QueueUpload(sessionID, filePath, posterPath, incidentID, incidentType, storeID, callbackURL string) error {
	upload := &PendingUpload{
		SessionID:    sessionID,
		FilePath:     filePath,
		PosterPath:   posterPath,
		IncidentID:   incidentID,
		IncidentType: incidentType,
		StoreID:      storeID,
		CallbackURL:  callbackURL,
		CreatedAt:    time.Now(),
	}
	
	select {
	case rm.uploadQueue <- upload:
		log.WithFields(logrus.Fields{
			"module":        "recording",
			"session_id":    sessionID,
			"incident_id":   incidentID,
			"incident_type": incidentType,
			"store_id":      storeID,
			"queue_size":    len(rm.uploadQueue),
		}).Infoln("Upload queued for background processing")
		return nil
	default:
		return fmt.Errorf("upload queue is full")
	}
}

// StopRecordingWithNext stops current recording and optionally starts next recording
func (rm *RecordingManager) StopRecordingWithNext(streamID, channelID, incidentID, storeID, incidentType, callbackURL string, isStartNext bool) (*RecordingSession, *RecordingSession, error) {
	stopTime := time.Now()

	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	var newSession *RecordingSession

	sessionKey := fmt.Sprintf("%s_%s", streamID, channelID)
	session, exists := rm.sessions[sessionKey]
	if !exists {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"stream_id":  streamID,
			"channel_id": channelID,
			"action":     "stop_with_next",
			"is_start_next": isStartNext,
		}).Warnln("No active recording found for stream")

		if isStartNext {
			newSession, err := rm.startRecordingInternal(streamID, channelID, "", false, "", "", 30, "h264")
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "recording",
					"stream": streamID,
					"channel": channelID,
					"error": err.Error(),
				}).Errorln("Failed to start new recording after missing session")
				return nil, nil, nil
			}
			return nil, newSession, nil
		}
		
		return nil, nil, nil
	}

	session.mutex.Lock()
	currentStatus := session.Status
	if currentStatus != "recording" && currentStatus != "failed" {
		session.mutex.Unlock()
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"status":     currentStatus,
		}).Warnln("Recording is not active, but continuing with processing")
	} else {
		session.IncidentID = incidentID
		session.CallbackURL = callbackURL
		session.Status = "stopping"
		originalRTSPURL := session.RTSPURL
		originalUseFFmpeg := session.UseFFmpeg
		originalFPS := session.FPS
		originalCodec := session.Codec
		session.mutex.Unlock()

		session.cancel()

		session.mutex.Lock()
		if session.Status == "stopping" {
			session.Status = "stopped"
		}
		session.mutex.Unlock()

		if isStartNext {
			go func() {
				newSession, err := rm.startRecordingInternal(streamID, channelID, originalRTSPURL, originalUseFFmpeg, "", "", originalFPS, originalCodec)
				if err != nil {
					log.WithFields(logrus.Fields{
						"module": "recording",
						"stream": streamID,
						"channel": channelID,
						"error": err.Error(),
					}).Errorln("Failed to start new recording after stop")
				} else {
					log.WithFields(logrus.Fields{
						"module":        "recording",
						"old_session":   session.ID,
						"new_session":   newSession.ID,
						"stream":        streamID,
						"channel":       channelID,
					}).Infoln("Started new recording after stop")
				}
			}()
		}
	}

	delete(rm.sessions, sessionKey)

	stopDuration := time.Since(stopTime)
	session.StopDuration = stopDuration

	if _, err := os.Stat(session.FilePath); err == nil {
		if err := rm.organizeVSSFiles(session, incidentType, storeID); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"error":      err.Error(),
			}).Errorln("Failed to organize files into VSS structure")
		}

		log.WithFields(logrus.Fields{
			"module":        "recording",
			"session_id":    session.ID,
			"stream":        streamID,
			"channel":       channelID,
			"incident_id":   incidentID,
			"incident_type": incidentType,
			"store_id":      storeID,
			"duration":      time.Since(session.StartTime).String(),
			"is_start_next": isStartNext,
			"prev_status":   currentStatus,
			"stop_duration": stopDuration.String(),
		}).Infoln("Recording stopped and organized into VSS structure")

		if err := rm.QueueUpload(session.ID, session.FilePath, session.PosterPath, incidentID, incidentType, storeID, callbackURL); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"error":      err.Error(),
			}).Errorln("Failed to queue upload")
		}
	} else {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"file_path":  session.FilePath,
		}).Warnln("Video file not found, skipping VSS processing")
	}

	return session, newSession, nil
}

// StopRecordingNoIncidentWithNext stops recording without incident details and optionally starts next recording
func (rm *RecordingManager) StopRecordingNoIncidentWithNext(streamID, channelID string, isStartNext bool) (*RecordingSession, *RecordingSession, error) {
	stopTime := time.Now()

	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	var newSession *RecordingSession

	sessionKey := fmt.Sprintf("%s_%s", streamID, channelID)
	session, exists := rm.sessions[sessionKey]
	if !exists {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"stream_id":  streamID,
			"channel_id": channelID,
			"action":     "stop_no_incident",
			"is_start_next": isStartNext,
		}).Warnln("No active recording found for stream")

		if isStartNext {
			newSession, err := rm.startRecordingInternal(streamID, channelID, "", false, "", "", 30, "h264")
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "recording",
					"stream": streamID,
					"channel": channelID,
					"error": err.Error(),
				}).Errorln("Failed to start new recording after missing session")
				return nil, nil, nil
			}
			return nil, newSession, nil
		}
		
		return nil, nil, nil
	}

	session.mutex.Lock()
	currentStatus := session.Status
	if currentStatus != "recording" && currentStatus != "failed" {
		session.mutex.Unlock()
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"status":     currentStatus,
		}).Warnln("Recording is not active, but continuing with cleanup")
	} else {
		session.Status = "stopping"
		originalRTSPURL := session.RTSPURL
		originalUseFFmpeg := session.UseFFmpeg
		originalFPS := session.FPS
		originalCodec := session.Codec
		session.mutex.Unlock()

		session.cancel()

		session.mutex.Lock()
		if session.Status == "stopping" {
			session.Status = "stopped"
		}
		session.mutex.Unlock()

		if isStartNext {
			go func() {
				newSession, err := rm.startRecordingInternal(streamID, channelID, originalRTSPURL, originalUseFFmpeg, "", "", originalFPS, originalCodec)
				if err != nil {
					log.WithFields(logrus.Fields{
						"module": "recording",
						"stream": streamID,
						"channel": channelID,
						"error": err.Error(),
					}).Errorln("Failed to start new recording after cleanup stop")
				} else {
					log.WithFields(logrus.Fields{
						"module":        "recording",
						"old_session":   session.ID,
						"new_session":   newSession.ID,
						"stream":        streamID,
						"channel":       channelID,
					}).Infoln("Started new recording after cleanup stop")
				}
			}()
		}
	}

	delete(rm.sessions, sessionKey)

	stopDuration := time.Since(stopTime)
	session.StopDuration = stopDuration

	log.WithFields(logrus.Fields{
		"module":        "recording",
		"session_id":    session.ID,
		"stream":        streamID,
		"channel":       channelID,
		"duration":      time.Since(session.StartTime).String(),
		"is_start_next": isStartNext,
		"prev_status":   currentStatus,
		"stop_duration": stopDuration.String(),
	}).Infoln("Recording stopped without incident")

	go func() {
		if err := rm.cleanupRecordingFiles(session); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"error":      err.Error(),
			}).Errorln("Failed to cleanup recording files")
		}
	}()

	return session, newSession, nil
}

// cleanupRecordingFiles deletes recording files when no incident is generated
func (rm *RecordingManager) cleanupRecordingFiles(session *RecordingSession) error {
	var deletedFiles []string
	var errors []string

	if _, err := os.Stat(session.FilePath); err == nil {
		if err := os.Remove(session.FilePath); err != nil {
			errors = append(errors, fmt.Sprintf("video: %v", err))
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"file_path":  session.FilePath,
				"error":      err.Error(),
			}).Errorln("Failed to delete video file")
		} else {
			deletedFiles = append(deletedFiles, session.FilePath)
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"file_path":  session.FilePath,
			}).Infoln("Video file deleted (no incident)")
		}
	}

	if _, err := os.Stat(session.PosterPath); err == nil {
		if err := os.Remove(session.PosterPath); err != nil {
			errors = append(errors, fmt.Sprintf("poster: %v", err))
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"poster_path": session.PosterPath,
				"error":      err.Error(),
			}).Warnln("Failed to delete poster file")
		} else {
			deletedFiles = append(deletedFiles, session.PosterPath)
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"poster_path": session.PosterPath,
			}).Infoln("Poster file deleted (no incident)")
		}
	}

	rm.cleanupSessionDirectories(session)

	if len(errors) > 0 {
		return fmt.Errorf("cleanup errors: %s", strings.Join(errors, ", "))
	}

	log.WithFields(logrus.Fields{
		"module":       "recording",
		"session_id":   session.ID,
		"deleted_files": len(deletedFiles),
	}).Infoln("Session cleanup completed")

	return nil
}

// cleanupSessionDirectories removes empty directories after file deletion
func (rm *RecordingManager) cleanupSessionDirectories(session *RecordingSession) {
	directories := make(map[string]bool)
	
	if session.FilePath != "" {
		directories[filepath.Dir(session.FilePath)] = true
	}
	if session.PosterPath != "" {
		directories[filepath.Dir(session.PosterPath)] = true
	}

	for dir := range directories {
		rm.removeEmptyDirectoryChain(dir)
	}
}

// removeEmptyDirectoryChain removes empty directories up the chain
func (rm *RecordingManager) removeEmptyDirectoryChain(dirPath string) {
	if dirPath == "." || dirPath == "/" || dirPath == "temp" || dirPath == "vss" {
		return
	}

	if rm.removeEmptyDirectory(dirPath) {
		parentDir := filepath.Dir(dirPath)
		if parentDir != dirPath {
			rm.removeEmptyDirectoryChain(parentDir)
		}
	}
}

// cleanupRecordingFilesComplete completely removes all files and directories for a session
func (rm *RecordingManager) cleanupRecordingFilesComplete(sessionID, filePath, posterPath string) error {
	var deletedFiles []string
	var errors []string

	log.WithFields(logrus.Fields{
		"module":     "recording",
		"session_id": sessionID,
		"file_path":  filePath,
		"poster_path": posterPath,
	}).Infoln("Starting complete file cleanup for removed recording")

	if filePath != "" {
		if _, err := os.Stat(filePath); err == nil {
			if err := os.Remove(filePath); err != nil {
				errors = append(errors, fmt.Sprintf("video file %s: %v", filePath, err))
				log.WithFields(logrus.Fields{
					"module":     "recording",
					"session_id": sessionID,
					"file_path":  filePath,
					"error":      err.Error(),
				}).Errorln("Failed to delete video file")
			} else {
				deletedFiles = append(deletedFiles, filePath)
				log.WithFields(logrus.Fields{
					"module":     "recording",
					"session_id": sessionID,
					"file_path":  filePath,
				}).Infoln("Video file deleted")
			}
		}
	}

	if posterPath != "" {
		if _, err := os.Stat(posterPath); err == nil {
			if err := os.Remove(posterPath); err != nil {
				errors = append(errors, fmt.Sprintf("poster file %s: %v", posterPath, err))
				log.WithFields(logrus.Fields{
					"module":     "recording",
					"session_id": sessionID,
					"poster_path": posterPath,
					"error":      err.Error(),
				}).Errorln("Failed to delete poster file")
			} else {
				deletedFiles = append(deletedFiles, posterPath)
				log.WithFields(logrus.Fields{
					"module":     "recording",
					"session_id": sessionID,
					"poster_path": posterPath,
				}).Infoln("Poster file deleted")
			}
		}
	}

	tempSessionDir := filepath.Join("temp", "recordings", sessionID)
	if _, err := os.Stat(tempSessionDir); err == nil {
		entries, err := os.ReadDir(tempSessionDir)
		if err == nil {
			for _, entry := range entries {
				if !entry.IsDir() {
					entryPath := filepath.Join(tempSessionDir, entry.Name())
					if err := os.Remove(entryPath); err != nil {
						errors = append(errors, fmt.Sprintf("session file %s: %v", entryPath, err))
						log.WithFields(logrus.Fields{
							"module":     "recording",
							"session_id": sessionID,
							"file":       entryPath,
							"error":      err.Error(),
						}).Errorln("Failed to delete session file")
					} else {
						deletedFiles = append(deletedFiles, entryPath)
						log.WithFields(logrus.Fields{
							"module":     "recording",
							"session_id": sessionID,
							"file":       entryPath,
						}).Infoln("Session file deleted")
					}
				}
			}
		}

		if err := os.Remove(tempSessionDir); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": sessionID,
				"directory":  tempSessionDir,
				"error":      err.Error(),
			}).Warnln("Failed to remove session directory")
		} else {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": sessionID,
				"directory":  tempSessionDir,
			}).Infoln("Session directory removed")
		}
	}

	if filePath != "" {
		rm.removeEmptyDirectoryChain(filepath.Dir(filePath))
	}
	if posterPath != "" {
		rm.removeEmptyDirectoryChain(filepath.Dir(posterPath))
	}

	if len(errors) > 0 {
		log.WithFields(logrus.Fields{
			"module":       "recording",
			"session_id":   sessionID,
			"deleted_files": len(deletedFiles),
			"errors":       len(errors),
		}).Warnln("Complete file cleanup finished with some errors")
		return fmt.Errorf("cleanup errors: %s", strings.Join(errors, ", "))
	}

	log.WithFields(logrus.Fields{
		"module":       "recording",
		"session_id":   sessionID,
		"deleted_files": len(deletedFiles),
	}).Infoln("Complete file cleanup finished successfully")

	return nil
}

// processVSSRecording handles poster generation, upload, and callback
func (rm *RecordingManager) processVSSRecording(session *RecordingSession) error {
	log.WithFields(logrus.Fields{
		"module":      "recording",
		"session_id":  session.ID,
		"video_path":  session.FilePath,
		"poster_path": session.PosterPath,
		"is_vss_video": strings.Contains(session.FilePath, "vss/"),
		"is_vss_poster": strings.Contains(session.PosterPath, "vss/"),
	}).Infoln("About to upload files - verifying VSS paths")

	if err := rm.generateVSSPoster(session); err != nil {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"error":      err.Error(),
		}).Warnln("Failed to generate VSS poster")
	}

	clipURL, posterURL, err := uploadVSSToDigitalOcean(session)
	if err != nil {
		return fmt.Errorf("failed to upload VSS files: %v", err)
	}

	if session.CallbackURL != "" {
		if err := sendVSSCallback(session.CallbackURL, session.IncidentID, clipURL, posterURL); err != nil {
			log.WithFields(logrus.Fields{
				"module":      "recording",
				"session_id":  session.ID,
				"callback_url": session.CallbackURL,
				"error":       err.Error(),
			}).Errorln("Failed to send VSS callback")
		}
	}

	return nil
}

// generateVSSPoster generates a poster from video generation images
func (rm *RecordingManager) generateVSSPoster(session *RecordingSession) error {
	if _, err := os.Stat(session.FilePath); os.IsNotExist(err) {
		return fmt.Errorf("video file does not exist: %s", session.FilePath)
	}

	durationCmd := exec.Command("ffprobe",
		"-v", "quiet",
		"-show_entries", "format=duration",
		"-of", "csv=p=0",
		session.FilePath,
	)
	
	durationOutput, err := durationCmd.Output()
	if err != nil {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"error":      err.Error(),
		}).Warnln("Failed to get video duration, using default extraction time")
	}

	extractTime := "00:00:01"
	if err == nil && len(durationOutput) > 0 {
		if duration, parseErr := time.ParseDuration(strings.TrimSpace(string(durationOutput)) + "s"); parseErr == nil {
			halfDuration := duration / 2
			if halfDuration > time.Second {
				extractTime = fmt.Sprintf("%02d:%02d:%02d", 
					int(halfDuration.Hours()), 
					int(halfDuration.Minutes())%60, 
					int(halfDuration.Seconds())%60)
			}
		}
	}

	log.WithFields(logrus.Fields{
		"module":       "recording",
		"session_id":   session.ID,
		"extract_time": extractTime,
		"video_path":   session.FilePath,
		"poster_path":  session.PosterPath,
	}).Infoln("Generating poster from video")

	cmd := exec.Command("ffmpeg",
		"-i", session.FilePath,
		"-ss", extractTime,
		"-vframes", "1",
		"-q:v", "2",
		"-y",
		"-an",
		"-f", "image2",
		session.PosterPath,
	)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to generate poster: %v, stderr: %s", err, stderr.String())
	}

	if _, err := os.Stat(session.PosterPath); err != nil {
		return fmt.Errorf("poster file was not created: %s", session.PosterPath)
	}

	log.WithFields(logrus.Fields{
		"module":      "recording",
		"session_id":  session.ID,
		"poster_path": session.PosterPath,
		"extract_time": extractTime,
	}).Infoln("VSS Poster generated successfully")

	return nil
}

// RemoveRecording completely stops recording and removes stream
func (rm *RecordingManager) RemoveRecording(streamID, channelID string) error {
	rm.setStopStartActive(true)
	defer rm.setStopStartActive(false)
	
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	sessionKey := fmt.Sprintf("%s_%s", streamID, channelID)
	session, exists := rm.sessions[sessionKey]
	
	if exists {
		session.mutex.Lock()
		session.Continuous = false

		if session.Status == "recording" || session.Status == "failed" {
			session.cancel()
		}

		filePath := session.FilePath
		posterPath := session.PosterPath
		sessionID := session.ID
		
		session.mutex.Unlock()

		delete(rm.sessions, sessionKey)
		
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": sessionID,
			"stream":     streamID,
			"channel":    channelID,
			"file_path":  filePath,
			"poster_path": posterPath,
		}).Infoln("Removing recording session and cleaning up files")

		go func() {
			if err := rm.cleanupRecordingFilesComplete(sessionID, filePath, posterPath); err != nil {
				log.WithFields(logrus.Fields{
					"module":     "recording",
					"session_id": sessionID,
					"error":      err.Error(),
				}).Errorln("Failed to cleanup recording files during removal")
			}
		}()
	} else {
		log.WithFields(logrus.Fields{
			"module":  "recording",
			"stream":  streamID,
			"channel": channelID,
		}).Infoln("No active recording session found, proceeding with config cleanup")
	}

	if err := Storage.StreamChannelDelete(streamID, channelID); err != nil {
		log.WithFields(logrus.Fields{
			"module":  "recording",
			"stream":  streamID,
			"channel": channelID,
			"error":   err.Error(),
		}).Errorln("Failed to remove channel from config")
		return fmt.Errorf("failed to remove channel from config: %v", err)
	}

	log.WithFields(logrus.Fields{
		"module":  "recording",
		"stream":  streamID,
		"channel": channelID,
	}).Infoln("Channel removed from configuration")

	if streamInfo, err := Storage.StreamInfo(streamID); err == nil {
		if len(streamInfo.Channels) == 0 {
			if err := Storage.StreamDelete(streamID); err != nil {
				log.WithFields(logrus.Fields{
					"module": "recording",
					"stream": streamID,
					"error":  err.Error(),
				}).Warnln("Failed to remove empty stream from config")
			} else {
				log.WithFields(logrus.Fields{
					"module": "recording",
					"stream": streamID,
				}).Infoln("Empty stream removed from configuration")
			}
		}
	}

	log.WithFields(logrus.Fields{
		"module":  "recording",
		"stream":  streamID,
		"channel": channelID,
	}).Infoln("Recording completely removed - session stopped, files cleaned up, config updated")

	return nil
}

// GetRecordingStatus returns the status of a recording session
func (rm *RecordingManager) GetRecordingStatus(streamID, channelID string) (*RecordingSession, error) {
	rm.mutex.RLock()
	defer rm.mutex.RUnlock()

	sessionKey := fmt.Sprintf("%s_%s", streamID, channelID)
	session, exists := rm.sessions[sessionKey]
	if !exists {
		return nil, fmt.Errorf("no recording session found for stream %s channel %s", streamID, channelID)
	}

	return session, nil
}

// ListRecordingSessions returns all recording sessions
func (rm *RecordingManager) ListRecordingSessions() map[string]*RecordingSession {
	rm.mutex.RLock()
	defer rm.mutex.RUnlock()

	sessions := make(map[string]*RecordingSession)
	for k, v := range rm.sessions {
		sessions[k] = v
	}
	return sessions
}

// CleanupSession removes a completed session from memory
func (rm *RecordingManager) CleanupSession(streamID, channelID string) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	sessionKey := fmt.Sprintf("%s_%s", streamID, channelID)
	delete(rm.sessions, sessionKey)
}

// DeletePreviousRecordingFiles deletes files from a previous recording session
func (rm *RecordingManager) DeletePreviousRecordingFiles(sessionID string) error {
	rm.waitForStopStartCompletion()
	
	log.WithFields(logrus.Fields{
		"module":     "recording",
		"session_id": sessionID,
	}).Infoln("Starting deletion of previous recording files")

	tempDir := filepath.Join("temp", "recordings", sessionID)
	
	var deletedFiles []string
	var errors []string

	if _, err := os.Stat(tempDir); err == nil {
		entries, err := os.ReadDir(tempDir)
		if err != nil {
			return fmt.Errorf("failed to read session directory: %v", err)
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				filePath := filepath.Join(tempDir, entry.Name())

				if rm.isFileInActiveSession(filePath) {
					log.WithFields(logrus.Fields{
						"module":     "recording",
						"session_id": sessionID,
						"file":       filePath,
					}).Debugln("Skipping deletion of active session file")
					continue
				}

				if err := os.Remove(filePath); err != nil {
					errors = append(errors, fmt.Sprintf("file %s: %v", filePath, err))
					log.WithFields(logrus.Fields{
						"module":     "recording",
						"session_id": sessionID,
						"file":       filePath,
						"error":      err.Error(),
					}).Errorln("Failed to delete previous recording file")
				} else {
					deletedFiles = append(deletedFiles, filePath)
					log.WithFields(logrus.Fields{
						"module":     "recording",
						"session_id": sessionID,
						"file":       filePath,
					}).Infoln("Deleted previous recording file")
				}
			}
		}

		if rm.removeEmptyDirectory(tempDir) {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": sessionID,
				"directory":  tempDir,
			}).Infoln("Removed empty session directory")
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("deletion errors: %s", strings.Join(errors, ", "))
	}

	log.WithFields(logrus.Fields{
		"module":       "recording",
		"session_id":   sessionID,
		"deleted_files": len(deletedFiles),
	}).Infoln("Previous recording files deletion completed")

	return nil
}
