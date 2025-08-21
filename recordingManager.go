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

// RecordingManager manages all recording sessions
type RecordingManager struct {
	sessions map[string]*RecordingSession
	mutex    sync.RWMutex

	autoStopTicker *time.Ticker
	stopChecker    chan bool
}

var recordingManager *RecordingManager

func init() {
	recordingManager = &RecordingManager{
		sessions:    make(map[string]*RecordingSession),
		stopChecker: make(chan bool),
	}
	
	recordingManager.startAutoStopChecker()
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
	
	maxDuration := 90 * time.Second // 1 minute 30 seconds
	
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
			_, _, err := rm.StopRecordingNoIncidentWithNext(s.StreamID, s.ChannelID, true)
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
			session.PosterPath = vssPosterPath
		}
	}

	session.IncidentType = incidentType
	session.StoreID = storeID

	return nil
}

// StartRecording starts a new recording session
func (rm *RecordingManager) StartRecording(streamID, channelID, rtspURL string, useFFmpeg bool, incidentType, storeID string) (*RecordingSession, error) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	if !Storage.StreamChannelExist(streamID, channelID) {
		log.WithFields(logrus.Fields{
			"module":  "recording",
			"stream":  streamID,
			"channel": channelID,
		}).Infoln("Stream/channel not found, creating automatically")

		if _, err := Storage.StreamInfo(streamID); err != nil {
			streamConfig := StreamST{
				Name:     fmt.Sprintf("Recording Stream %s", streamID),
				Channels: make(map[string]ChannelST),
			}
			if err := Storage.StreamAdd(streamID, streamConfig); err != nil {
				return nil, fmt.Errorf("failed to create stream: %v", err)
			}
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

		time.Sleep(2 * time.Second)
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

	log.WithFields(logrus.Fields{
		"module":        "recording",
		"session_id":    sessionID,
		"stream":        streamID,
		"channel":       channelID,
		"method":        map[bool]string{true: "ffmpeg", false: "native"}[useFFmpeg],
		"incident_type": incidentType,
		"store_id":      storeID,
		"video_path":    videoPath,
		"poster_path":   posterPath,
		"temporary":     incidentType == "" || storeID == "",
		"auto_stop":     "45s",
		"max_duration":  "1m30s",
	}).Infoln("Recording started")

	return session, nil
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
			relativeTime = session.lastTimestamp + time.Millisecond*33
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
			if session.muxer != nil {
				session.muxer.WriteTrailer()
			}
			if session.file != nil {
				session.file.Close()
			}
			Storage.ClientDelete(session.StreamID, session.clientID, session.ChannelID)
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
				}).Infoln("Recording stopped by context")
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

// StopRecordingWithNext stops current recording and optionally starts next recording
func (rm *RecordingManager) StopRecordingWithNext(streamID, channelID, incidentID, storeID, incidentType, callbackURL string, isStartNext bool) (*RecordingSession, *RecordingSession, error) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	sessionKey := fmt.Sprintf("%s_%s", streamID, channelID)
	session, exists := rm.sessions[sessionKey]
	if !exists {
		return nil, nil, fmt.Errorf("no active recording found for stream %s channel %s", streamID, channelID)
	}

	session.mutex.Lock()
	currentStatus := session.Status
	if currentStatus != "recording" && currentStatus != "failed" {
		session.mutex.Unlock()
		return session, nil, fmt.Errorf("recording is not active (status: %s)", currentStatus)
	}

	session.IncidentID = incidentID
	session.CallbackURL = callbackURL
	session.Status = "stopping"
	originalRTSPURL := session.RTSPURL
	originalUseFFmpeg := session.UseFFmpeg
	session.mutex.Unlock()

	session.cancel()

	time.Sleep(2 * time.Second)

	session.mutex.Lock()
	if session.Status == "stopping" {
		session.Status = "stopped"
	}
	session.mutex.Unlock()

	delete(rm.sessions, sessionKey)

	if _, err := os.Stat(session.FilePath); err == nil {
		if err := rm.organizeVSSFiles(session, incidentType, storeID); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"error":      err.Error(),
			}).Errorln("Failed to organize files into VSS structure")
		}

		oldSession := *session

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
		}).Infoln("Recording stopped and organized into VSS structure")

		go func() {
			if err := rm.processVSSRecording(&oldSession); err != nil {
				log.WithFields(logrus.Fields{
					"module":     "recording",
					"session_id": oldSession.ID,
					"error":      err.Error(),
				}).Errorln("Failed to process VSS recording")
			}
		}()
	} else {
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"file_path":  session.FilePath,
		}).Warnln("Video file not found, skipping VSS processing")
	}

	var newSession *RecordingSession

	if isStartNext {
		go func() {
			time.Sleep(1 * time.Second)
			newSess, err := rm.StartRecording(streamID, channelID, originalRTSPURL, originalUseFFmpeg, "", "")
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
					"new_session":   newSess.ID,
					"stream":        streamID,
					"channel":       channelID,
				}).Infoln("Started new recording after stop")
				newSession = newSess
			}
		}()
	}

	return session, newSession, nil
}

// StopRecordingNoIncidentWithNext stops recording without incident details and optionally starts next recording
func (rm *RecordingManager) StopRecordingNoIncidentWithNext(streamID, channelID string, isStartNext bool) (*RecordingSession, *RecordingSession, error) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	sessionKey := fmt.Sprintf("%s_%s", streamID, channelID)
	session, exists := rm.sessions[sessionKey]
	if !exists {
		return nil, nil, fmt.Errorf("no active recording found for stream %s channel %s", streamID, channelID)
	}

	session.mutex.Lock()
	currentStatus := session.Status
	if currentStatus != "recording" && currentStatus != "failed" {
		session.mutex.Unlock()
		return session, nil, fmt.Errorf("recording is not active (status: %s)", currentStatus)
	}

	session.Status = "stopping"
	originalRTSPURL := session.RTSPURL
	originalUseFFmpeg := session.UseFFmpeg
	session.mutex.Unlock()

	session.cancel()

	time.Sleep(2 * time.Second)

	session.mutex.Lock()
	if session.Status == "stopping" {
		session.Status = "stopped"
	}
	session.mutex.Unlock()

	delete(rm.sessions, sessionKey)

	log.WithFields(logrus.Fields{
		"module":        "recording",
		"session_id":    session.ID,
		"stream":        streamID,
		"channel":       channelID,
		"duration":      time.Since(session.StartTime).String(),
		"is_start_next": isStartNext,
		"prev_status":   currentStatus,
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

	var newSession *RecordingSession

	if isStartNext {
		go func() {
			time.Sleep(1 * time.Second)
			newSess, err := rm.StartRecording(streamID, channelID, originalRTSPURL, originalUseFFmpeg, "", "")
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
					"new_session":   newSess.ID,
					"stream":        streamID,
					"channel":       channelID,
				}).Infoln("Started new recording after cleanup stop")
				newSession = newSess
			}
		}()
	}

	return session, newSession, nil
}

// cleanupRecordingFiles deletes recording files when no incident is generated
func (rm *RecordingManager) cleanupRecordingFiles(session *RecordingSession) error {
	if _, err := os.Stat(session.FilePath); err == nil {
		if err := os.Remove(session.FilePath); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"file_path":  session.FilePath,
				"error":      err.Error(),
			}).Errorln("Failed to delete video file")
			return fmt.Errorf("failed to delete video file: %v", err)
		}
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"file_path":  session.FilePath,
		}).Infoln("Video file deleted (no incident)")
	}

	if _, err := os.Stat(session.PosterPath); err == nil {
		if err := os.Remove(session.PosterPath); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"poster_path": session.PosterPath,
				"error":      err.Error(),
			}).Warnln("Failed to delete poster file")
		} else {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"poster_path": session.PosterPath,
			}).Infoln("Poster file deleted (no incident)")
		}
	}

	tempDir := filepath.Dir(session.FilePath)
	if strings.Contains(tempDir, "temp/recordings/") {
		if err := os.Remove(tempDir); err != nil {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"temp_dir":   tempDir,
			}).Debugln("Temporary directory cleanup (may not be empty)")
		} else {
			log.WithFields(logrus.Fields{
				"module":     "recording",
				"session_id": session.ID,
				"temp_dir":   tempDir,
			}).Infoln("Temporary directory cleaned up")
		}
	}

	return nil
}

// processVSSRecording handles poster generation, upload, and callback
func (rm *RecordingManager) processVSSRecording(session *RecordingSession) error {
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
	// TODO: This should use one of the images from video generation process
	// For now, we'll extract a frame from the video as fallback

	if _, err := os.Stat(session.FilePath); os.IsNotExist(err) {
		return fmt.Errorf("video file does not exist: %s", session.FilePath)
	}

	cmd := exec.Command("ffmpeg",
		"-i", session.FilePath,
		"-ss", "00:00:05", // Extract frame at 5 seconds
		"-vframes", "1",
		"-q:v", "2", // High quality
		"-y", // Overwrite output file
		"-an", // Disable audio
		session.PosterPath,
	)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to generate poster: %v", err)
	}

	log.WithFields(logrus.Fields{
		"module":      "recording",
		"session_id":  session.ID,
		"poster_path": session.PosterPath,
	}).Infoln("VSS Poster generated successfully")

	return nil
}

// RemoveRecording completely stops recording and removes stream
func (rm *RecordingManager) RemoveRecording(streamID, channelID string) error {
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
		session.mutex.Unlock()

		time.Sleep(2 * time.Second)

		delete(rm.sessions, sessionKey)
		
		log.WithFields(logrus.Fields{
			"module":     "recording",
			"session_id": session.ID,
			"stream":     streamID,
			"channel":    channelID,
		}).Infoln("VSS Recording session removed")
	}

	// Remove stream/channel from configuration
	if err := Storage.StreamChannelDelete(streamID, channelID); err != nil {
		log.WithFields(logrus.Fields{
			"module":  "recording",
			"stream":  streamID,
			"channel": channelID,
			"error":   err.Error(),
		}).Warnln("Failed to remove channel from config")
	}

	// If no more channels, remove entire stream
	if streamInfo, err := Storage.StreamInfo(streamID); err == nil {
		if len(streamInfo.Channels) == 0 {
			if err := Storage.StreamDelete(streamID); err != nil {
				log.WithFields(logrus.Fields{
					"module": "recording",
					"stream": streamID,
					"error":  err.Error(),
				}).Warnln("Failed to remove stream from config")
			}
		}
	}

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
