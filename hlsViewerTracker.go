package main

import (
	"fmt"
	"sync"
	"time"
	"github.com/sirupsen/logrus"
)

// HLSViewer represents an HLS viewer session
type HLSViewer struct {
	ID          string    `json:"id"`
	StreamID    string    `json:"stream_id"`
	ChannelID   string    `json:"channel_id"`
	ClientIP    string    `json:"client_ip"`
	UserAgent   string    `json:"user_agent"`
	LastSeen    time.Time `json:"last_seen"`
	FirstSeen   time.Time `json:"first_seen"`
	RequestCount int      `json:"request_count"`
}

// HLSViewerTracker manages HLS viewer sessions
type HLSViewerTracker struct {
	mutex   sync.RWMutex
	viewers map[string]*HLSViewer // key: viewer ID
	cleanup *time.Ticker
}

var hlsTracker *HLSViewerTracker

func init() {
	hlsTracker = &HLSViewerTracker{
		viewers: make(map[string]*HLSViewer),
		cleanup: time.NewTicker(30 * time.Second),
	}

	go hlsTracker.startCleanup()
}

// startCleanup removes inactive HLS viewers
func (ht *HLSViewerTracker) startCleanup() {
	for range ht.cleanup.C {
		ht.mutex.Lock()
		now := time.Now()
		inactiveThreshold := 60 * time.Second // Consider inactive after 60 seconds
		
		for viewerID, viewer := range ht.viewers {
			if now.Sub(viewer.LastSeen) > inactiveThreshold {
				log.WithFields(logrus.Fields{
					"module":    "hls_tracker",
					"viewer_id": viewerID,
					"stream":    viewer.StreamID,
					"channel":   viewer.ChannelID,
					"inactive_for": now.Sub(viewer.LastSeen).String(),
				}).Debugln("Removing inactive HLS viewer")
				delete(ht.viewers, viewerID)
			}
		}
		ht.mutex.Unlock()
	}
}

// TrackViewer tracks or updates an HLS viewer
func (ht *HLSViewerTracker) TrackViewer(streamID, channelID, clientIP, userAgent string) string {
	ht.mutex.Lock()
	defer ht.mutex.Unlock()

	viewerID := generateViewerID(streamID, channelID, clientIP, userAgent)
	
	now := time.Now()
	if viewer, exists := ht.viewers[viewerID]; exists {
		viewer.LastSeen = now
		viewer.RequestCount++
		log.WithFields(logrus.Fields{
			"module":    "hls_tracker",
			"viewer_id": viewerID,
			"stream":    streamID,
			"channel":   channelID,
			"requests":  viewer.RequestCount,
		}).Debugln("Updated HLS viewer activity")
	} else {
		ht.viewers[viewerID] = &HLSViewer{
			ID:           viewerID,
			StreamID:     streamID,
			ChannelID:    channelID,
			ClientIP:     clientIP,
			UserAgent:    userAgent,
			FirstSeen:    now,
			LastSeen:     now,
			RequestCount: 1,
		}
		log.WithFields(logrus.Fields{
			"module":    "hls_tracker",
			"viewer_id": viewerID,
			"stream":    streamID,
			"channel":   channelID,
			"client_ip": clientIP,
		}).Infoln("New HLS viewer detected")
	}
	
	return viewerID
}

// GetViewerCount returns the number of active HLS viewers for a stream/channel
func (ht *HLSViewerTracker) GetViewerCount(streamID, channelID string) int {
	ht.mutex.RLock()
	defer ht.mutex.RUnlock()
	
	count := 0
	for _, viewer := range ht.viewers {
		if viewer.StreamID == streamID && viewer.ChannelID == channelID {
			count++
		}
	}
	return count
}

// GetViewers returns all active HLS viewers for a stream/channel
func (ht *HLSViewerTracker) GetViewers(streamID, channelID string) []*HLSViewer {
	ht.mutex.RLock()
	defer ht.mutex.RUnlock()
	
	var viewers []*HLSViewer
	for _, viewer := range ht.viewers {
		if viewer.StreamID == streamID && viewer.ChannelID == channelID {
			viewers = append(viewers, viewer)
		}
	}
	return viewers
}

// RemoveViewer manually removes a viewer (for testing or explicit disconnect)
func (ht *HLSViewerTracker) RemoveViewer(viewerID string) bool {
	ht.mutex.Lock()
	defer ht.mutex.Unlock()
	
	if _, exists := ht.viewers[viewerID]; exists {
		delete(ht.viewers, viewerID)
		return true
	}
	return false
}

// generateViewerID creates a unique ID for an HLS viewer
func generateViewerID(streamID, channelID, clientIP, userAgent string) string {
	// Use a combination of factors to create a unique viewer ID
	// This helps distinguish between different tabs/browsers from the same IP
	data := streamID + ":" + channelID + ":" + clientIP + ":" + userAgent

	hash := 0
	for _, char := range data {
		hash = hash*31 + int(char)
	}

	timestamp := time.Now().Unix() / 300
	
	return fmt.Sprintf("hls_%d_%d", hash, timestamp)
}

// GetAllViewerStats returns comprehensive viewer statistics
func (ht *HLSViewerTracker) GetAllViewerStats() map[string]interface{} {
	ht.mutex.RLock()
	defer ht.mutex.RUnlock()
	
	stats := map[string]interface{}{
		"total_hls_viewers": len(ht.viewers),
		"streams": make(map[string]map[string]interface{}),
	}
	
	streamStats := make(map[string]map[string]int)
	
	for _, viewer := range ht.viewers {
		streamKey := viewer.StreamID
		if streamStats[streamKey] == nil {
			streamStats[streamKey] = make(map[string]int)
		}
		streamStats[streamKey][viewer.ChannelID]++
	}
	
	stats["streams"] = streamStats
	return stats
}
