package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

// VSSCallbackPayload represents the callback payload for VSS
type VSSCallbackPayload struct {
	IncidentID string `json:"incidentId"`
	ClipURL    string `json:"clipUrl"`
	PosterURL  string `json:"posterUrl"`
}

// sendVSSCallback sends the callback to the provided URL
func sendVSSCallback(callbackURL, incidentID, clipURL, posterURL string) error {
	payload := VSSCallbackPayload{
		IncidentID: incidentID,
		ClipURL:    clipURL,
		PosterURL:  posterURL,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal callback payload: %v", err)
	}

	log.WithFields(logrus.Fields{
		"module":      "vss_callback",
		"callback_url": callbackURL,
		"incident_id": incidentID,
		"clip_url":    clipURL,
		"poster_url":  posterURL,
	}).Infoln("Sending VSS callback")

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequest("POST", callbackURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create callback request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send callback request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("callback returned non-success status: %d", resp.StatusCode)
	}

	log.WithFields(logrus.Fields{
		"module":        "vss_callback",
		"callback_url":  callbackURL,
		"incident_id":   incidentID,
		"response_code": resp.StatusCode,
	}).Infoln("VSS callback sent successfully")

	return nil
}
