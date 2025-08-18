package main

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/deepch/vdk/format/rtspv2"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// StreamTestResult represents the result of a stream test
type StreamTestResult struct {
	Success     bool   `json:"success"`
	Message     string `json:"message"`
	ErrorCode   string `json:"error_code,omitempty"`
	Details     string `json:"details,omitempty"`
	TestSteps   []TestStep `json:"test_steps"`
	StreamInfo  *StreamInfo `json:"stream_info,omitempty"`
}

// TestStep represents each step in the testing process
type TestStep struct {
	Step    string `json:"step"`
	Status  string `json:"status"` // "success", "failed", "warning"
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// StreamInfo contains information about the stream if successful
type StreamInfo struct {
	URL         string   `json:"url"`
	VideoCodec  string   `json:"video_codec,omitempty"`
	AudioCodec  string   `json:"audio_codec,omitempty"`
	Resolution  string   `json:"resolution,omitempty"`
	FPS         int      `json:"fps,omitempty"`
	Duration    string   `json:"test_duration"`
	PacketCount int      `json:"packet_count"`
}

// StreamTestRequest represents the request payload
type StreamTestRequest struct {
	URL string `json:"url" binding:"required"`
}

// HTTPAPIServerStreamTest tests if a stream URL is accessible and working
func HTTPAPIServerStreamTest(c *gin.Context) {
	requestLogger := log.WithFields(logrus.Fields{
		"module": "http_stream_test",
		"func":   "HTTPAPIServerStreamTest",
	})

	var payload StreamTestRequest
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.IndentedJSON(400, StreamTestResult{
			Success:   false,
			Message:   "Invalid request format. URL is required.",
			ErrorCode: "INVALID_REQUEST",
			Details:   err.Error(),
		})
		return
	}

	if payload.URL == "" {
		c.IndentedJSON(400, StreamTestResult{
			Success:   false,
			Message:   "Stream URL is required",
			ErrorCode: "MISSING_URL",
		})
		return
	}

	requestLogger.WithField("url_host", extractHostFromURL(payload.URL)).Info("Testing stream")

	// Perform the stream test
	result := testStreamConnection(payload.URL)
	
	if result.Success {
		c.IndentedJSON(200, result)
	} else {
		c.IndentedJSON(422, result) // Unprocessable Entity for stream issues
	}
}

// extractHostFromURL safely extracts just the host for logging (without credentials)
func extractHostFromURL(streamURL string) string {
	if parsedURL, err := url.Parse(streamURL); err == nil {
		return parsedURL.Host
	}
	return "unknown"
}

// getCodecName converts codec type to readable name
func getCodecName(codecType string) string {
	codecType = strings.ToLower(codecType)
	
	// Handle different H.264 variations
	if strings.Contains(codecType, "h264") || 
	   strings.Contains(codecType, "avc") ||
	   strings.Contains(codecType, "h.264") {
		return "h264"
	}
	
	// Handle different H.265 variations  
	if strings.Contains(codecType, "h265") || 
	   strings.Contains(codecType, "hevc") ||
	   strings.Contains(codecType, "h.265") {
		return "h265"
	}
	
	// Handle audio codecs
	if strings.Contains(codecType, "aac") {
		return "aac"
	}
	if strings.Contains(codecType, "pcm") || strings.Contains(codecType, "pcma") || strings.Contains(codecType, "pcmu") {
		return "pcm"
	}
	if strings.Contains(codecType, "g711") {
		return "g711"
	}
	if strings.Contains(codecType, "g722") {
		return "g722"
	}
	
	// Return original if no match
	return codecType
}

// testStreamConnection performs comprehensive testing of the stream
func testStreamConnection(streamURL string) StreamTestResult {
	result := StreamTestResult{
		TestSteps: []TestStep{},
	}

	// Step 1: Parse URL
	step1 := TestStep{Step: "url_parsing", Status: "success", Message: "URL parsing"}
	parsedURL, err := url.Parse(streamURL)
	if err != nil {
		step1.Status = "failed"
		step1.Message = "Invalid URL format"
		step1.Details = err.Error()
		result.TestSteps = append(result.TestSteps, step1)
		result.Success = false
		result.Message = "Invalid URL format. Please check your RTSP URL syntax."
		result.ErrorCode = "INVALID_URL"
		return result
	}
	step1.Details = fmt.Sprintf("Protocol: %s, Host: %s, Port: %s", parsedURL.Scheme, parsedURL.Hostname(), parsedURL.Port())
	result.TestSteps = append(result.TestSteps, step1)

	// Validate RTSP scheme
	if !strings.HasPrefix(strings.ToLower(parsedURL.Scheme), "rtsp") {
		result.TestSteps = append(result.TestSteps, TestStep{
			Step:    "protocol_validation",
			Status:  "failed",
			Message: "Unsupported protocol",
			Details: fmt.Sprintf("Expected RTSP protocol, got: %s", parsedURL.Scheme),
		})
		result.Success = false
		result.Message = "Only RTSP protocol is supported. Please use rtsp:// URLs."
		result.ErrorCode = "UNSUPPORTED_PROTOCOL"
		return result
	}

	// Step 2: DNS Resolution
	step2 := TestStep{Step: "dns_resolution", Status: "success", Message: "DNS resolution"}
	host := parsedURL.Hostname()
	if host == "" {
		step2.Status = "failed"
		step2.Message = "No hostname found in URL"
		result.TestSteps = append(result.TestSteps, step2)
		result.Success = false
		result.Message = "Invalid URL: no hostname specified."
		result.ErrorCode = "NO_HOSTNAME"
		return result
	}

	// Check if it's an IP address or needs DNS resolution
	if net.ParseIP(host) == nil {
		ips, err := net.LookupIP(host)
		if err != nil {
			step2.Status = "failed"
			step2.Message = "DNS resolution failed"
			step2.Details = err.Error()
			result.TestSteps = append(result.TestSteps, step2)
			result.Success = false
			result.Message = fmt.Sprintf("Cannot resolve hostname '%s'. Please check if the hostname is correct and accessible.", host)
			result.ErrorCode = "DNS_RESOLUTION_FAILED"
			return result
		}
		step2.Details = fmt.Sprintf("Resolved to: %v", ips)
	} else {
		step2.Details = fmt.Sprintf("Using IP address: %s", host)
	}
	result.TestSteps = append(result.TestSteps, step2)

	// Step 3: Port connectivity
	step3 := TestStep{Step: "port_connectivity", Status: "success", Message: "Port connectivity"}
	port := parsedURL.Port()
	if port == "" {
		port = "554"
	}

	address := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", address, 10*time.Second)
	if err != nil {
		step3.Status = "failed"
		step3.Message = "Cannot connect to port"
		step3.Details = err.Error()
		result.TestSteps = append(result.TestSteps, step3)
		result.Success = false
		result.Message = fmt.Sprintf("Port %s is not opened on the remote host %s. Please make sure that your camera is connected properly and your router has been configured.", port, host)
		result.ErrorCode = "PORT_NOT_ACCESSIBLE"
		return result
	}
	conn.Close()
	step3.Details = fmt.Sprintf("Successfully connected to %s", address)
	result.TestSteps = append(result.TestSteps, step3)

	step4 := TestStep{Step: "rtsp_handshake", Status: "success", Message: "RTSP handshake"}
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rtspClient, err := rtspv2.Dial(rtspv2.RTSPClientOptions{
		URL:               streamURL,
		DisableAudio:      true,
		DialTimeout:       10 * time.Second,
		ReadWriteTimeout:  10 * time.Second,
		Debug:             false,
	})

	if err != nil {
		step4.Status = "failed"
		step4.Details = err.Error()
		
		errorStr := strings.ToLower(err.Error())
		
		if strings.Contains(errorStr, "401") || strings.Contains(errorStr, "unauthorized") || strings.Contains(errorStr, "authentication") {
			step4.Message = "Authentication failed"
			result.Message = "Authentication failed. Please make sure that username and password are correct."
			result.ErrorCode = "AUTHENTICATION_FAILED"
		} else if strings.Contains(errorStr, "404") || strings.Contains(errorStr, "not found") {
			step4.Message = "Stream path not found"
			result.Message = "The specified stream path was not found on the camera. Please verify the stream URL path is correct."
			result.ErrorCode = "STREAM_PATH_NOT_FOUND"
		} else if strings.Contains(errorStr, "timeout") {
			step4.Message = "Connection timeout"
			result.Message = "Connection timeout. The camera may be overloaded or have insufficient bandwidth."
			result.ErrorCode = "CONNECTION_TIMEOUT"
		} else if strings.Contains(errorStr, "refused") {
			step4.Message = "Connection refused"
			result.Message = "Connection refused by the camera. Please check if RTSP service is enabled on the camera."
			result.ErrorCode = "CONNECTION_REFUSED"
		} else {
			step4.Message = "RTSP handshake failed"
			result.Message = "Failed to establish RTSP connection. Please check your camera settings and network configuration."
			result.ErrorCode = "RTSP_HANDSHAKE_FAILED"
		}
		
		result.TestSteps = append(result.TestSteps, step4)
		result.Success = false
		result.Details = err.Error()
		return result
	}

	step4.Details = "RTSP handshake successful"
	result.TestSteps = append(result.TestSteps, step4)

	step5 := TestStep{Step: "stream_validation", Status: "success", Message: "Stream validation"}
	
	startTime := time.Now()
	packetCount := 0
	var streamInfo StreamInfo
	streamInfo.URL = streamURL
	
	streamTimeout := time.NewTimer(20 * time.Second)
	defer streamTimeout.Stop()

	codecReceived := false
	packetsReceived := false

	var fpsCalculationStart time.Time
	var frameCount int

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-streamTimeout.C:
				return
			case signal := <-rtspClient.Signals:
				if signal == rtspv2.SignalCodecUpdate {
					codecReceived = true
					log.WithFields(logrus.Fields{
						"module": "stream_test",
						"func":   "testStreamConnection",
					}).Debugf("Codec update signal received: %d codecs", len(rtspClient.CodecData))
					
					for i, codec := range rtspClient.CodecData {
						codecTypeStr := codec.Type().String()
						log.WithFields(logrus.Fields{
							"module": "stream_test",
							"func":   "testStreamConnection",
						}).Debugf("Updated Codec %d: Type=%s, IsVideo=%v, IsAudio=%v", i, codecTypeStr, codec.Type().IsVideo(), codec.Type().IsAudio())
						
						if codec.Type().IsVideo() {
							streamInfo.VideoCodec = getCodecName(codecTypeStr)
						} else if codec.Type().IsAudio() {
							streamInfo.AudioCodec = getCodecName(codecTypeStr)
						}
					}
				}
			case packet := <-rtspClient.OutgoingPacketQueue:
				if packet != nil {
					packetCount++
					packetsReceived = true

					if packet.Idx == 0 {
						frameCount++
						
						if fpsCalculationStart.IsZero() {
							fpsCalculationStart = time.Now()
						}
						
						elapsed := time.Since(fpsCalculationStart).Seconds()
						if frameCount >= 10 && elapsed >= 1.0 {
							calculatedFPS := int(float64(frameCount) / elapsed)
							if calculatedFPS >= 1 && calculatedFPS <= 120 {
								streamInfo.FPS = calculatedFPS
								log.WithFields(logrus.Fields{
									"module": "stream_test",
									"func":   "testStreamConnection",
								}).Debugf("Calculated FPS: %d (frames: %d, elapsed: %.2fs)", calculatedFPS, frameCount, elapsed)
							}
						}
					}

					if !codecReceived && len(rtspClient.CodecData) > 0 {
						codecReceived = true
						for i, codec := range rtspClient.CodecData {
							codecTypeStr := codec.Type().String()
							log.WithFields(logrus.Fields{
								"module": "stream_test",
								"func":   "testStreamConnection",
							}).Debugf("Packet-triggered Codec %d: Type=%s, IsVideo=%v, IsAudio=%v", i, codecTypeStr, codec.Type().IsVideo(), codec.Type().IsAudio())
							
							if codec.Type().IsVideo() {
								streamInfo.VideoCodec = getCodecName(codecTypeStr)
							} else if codec.Type().IsAudio() {
								streamInfo.AudioCodec = getCodecName(codecTypeStr)
							}
						}
					}
					if packetCount >= 30 {
						return
					}
				}
			}
		}
	}()

	select {
	case <-streamTimeout.C:
		rtspClient.Close()
		
		if !codecReceived && !packetsReceived {
			step5.Status = "failed"
			step5.Message = "No stream data received"
			step5.Details = "Camera sent a valid RTSP response, however no video stream data was received within 15 seconds"
			result.TestSteps = append(result.TestSteps, step5)
			result.Success = false
			result.Message = "Camera sent a valid RTSP response, however the video stream cannot be started. Please make sure that the stream URL is valid or your camera has the necessary upload bandwidth."
			result.ErrorCode = "NO_STREAM_DATA"
			return result
		} else if codecReceived && !packetsReceived {
			step5.Status = "warning"
			step5.Message = "Codec information received but no packets"
			step5.Details = "Stream setup successful but no video packets received within timeout"
		}
	case <-ctx.Done():
		rtspClient.Close()
		step5.Status = "failed"
		step5.Message = "Test cancelled"
		result.TestSteps = append(result.TestSteps, step5)
		result.Success = false
		result.Message = "Stream test was cancelled or timed out."
		result.ErrorCode = "TEST_CANCELLED"
		return result
	default:
		time.Sleep(3 * time.Second)
	}

	rtspClient.Close()

	duration := time.Since(startTime)
	streamInfo.Duration = duration.String()
	streamInfo.PacketCount = packetCount

	log.WithFields(logrus.Fields{
		"module": "stream_test",
		"func":   "testStreamConnection",
	}).Debugf("Final codec info - Video: %s, Audio: %s", streamInfo.VideoCodec, streamInfo.AudioCodec)

	if packetCount > 0 {
		step5.Details = fmt.Sprintf("Received %d packets in %v", packetCount, duration)
		if streamInfo.VideoCodec != "" {
			step5.Details += fmt.Sprintf(", Video codec: %s", streamInfo.VideoCodec)
		}
		result.TestSteps = append(result.TestSteps, step5)
		result.Success = true
		result.Message = "Stream test successful! The camera is accessible and streaming video."
		result.StreamInfo = &streamInfo
	} else if codecReceived {
		step5.Status = "warning"
		step5.Message = "Stream setup successful but limited data"
		step5.Details = "Codec information received but no video packets within test period"
		if streamInfo.VideoCodec != "" {
			step5.Details += fmt.Sprintf(", Video codec: %s", streamInfo.VideoCodec)
		}
		if streamInfo.FPS > 0 {
			step5.Details += fmt.Sprintf(", FPS: %d", streamInfo.FPS)
		}
		result.TestSteps = append(result.TestSteps, step5)
		result.Success = true
		result.Message = "Stream connection successful but limited video data received. The stream may work but could have bandwidth issues."
		result.StreamInfo = &streamInfo
	} else {
		step5.Status = "failed"
		step5.Message = "No stream data received"
		result.TestSteps = append(result.TestSteps, step5)
		result.Success = false
		result.Message = "Camera sent a valid RTSP response, however the video stream cannot be started. Please make sure that the stream URL is valid or your camera has the necessary upload bandwidth."
		result.ErrorCode = "NO_STREAM_DATA"
	}

	return result
}
