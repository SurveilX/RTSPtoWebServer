package main

import (
	"net/url"
	"testing"
)

// TestSRTStreamConnection_LiveSource is a real, functional test — not a mock — against a live
// local SRT publisher (ffmpeg acting as an SRT listener streaming a test pattern). It exists
// because this repo shipped with zero tests before this change; it also directly exercises the
// new code path added for SRT camera ingestion support, end to end, against real encoded video.
//
// Requires a local SRT source at srt://127.0.0.1:9501 (mode=caller, the default) — see the
// project's SRT verification notes for the exact ffmpeg command used to stand one up. Skips
// itself if nothing is listening, so it never fails CI in an environment without that source.
func TestSRTStreamConnection_LiveSource(t *testing.T) {
	streamURL := "srt://127.0.0.1:9501"

	parsedURL, err := url.Parse(streamURL)
	if err != nil {
		t.Fatalf("failed to parse test URL: %v", err)
	}

	result := testStreamConnection(streamURL)

	if !result.Success {
		t.Skipf("no live SRT source reachable at %s (result: %+v) — skipping, this needs a real local publisher", streamURL, result)
	}

	if result.StreamInfo == nil {
		t.Fatalf("expected StreamInfo on success, got nil. Result: %+v", result)
	}

	if result.StreamInfo.VideoCodec != "h264" {
		t.Errorf("expected VideoCodec=h264 (matching the real synthetic source), got %q", result.StreamInfo.VideoCodec)
	}

	if result.StreamInfo.Resolution != "640x480" {
		t.Errorf("expected Resolution=640x480 (matching the real synthetic source), got %q", result.StreamInfo.Resolution)
	}

	foundHandshakeStep := false
	for _, step := range result.TestSteps {
		if step.Step == "srt_handshake" && step.Status == "success" {
			foundHandshakeStep = true
		}
	}
	if !foundHandshakeStep {
		t.Errorf("expected a successful srt_handshake test step, got: %+v", result.TestSteps)
	}

	_ = parsedURL
}

// TestSRTStreamConnection_UnreachableTarget is the negative control: an SRT URL with nothing
// listening must fail cleanly, with a typed result and no panic/hang — this is the property the
// whole defensive design (bounded timeouts, recover(), guaranteed cleanup) exists to guarantee.
func TestSRTStreamConnection_UnreachableTarget(t *testing.T) {
	result := testStreamConnection("srt://127.0.0.1:19999?streamid=read:nonexistent")

	if result.Success {
		t.Fatalf("expected failure connecting to a port nothing listens on, got success: %+v", result)
	}

	if result.ErrorCode != "SRT_HANDSHAKE_FAILED" {
		t.Errorf("expected ErrorCode=SRT_HANDSHAKE_FAILED, got %q (full result: %+v)", result.ErrorCode, result)
	}
}

// TestSRTStreamConnection_MissingPort confirms the deliberate no-default-port validation (SRT,
// unlike RTSP, has no IANA-assigned default port, so guessing one would silently test the wrong
// thing) rejects cleanly instead of guessing.
func TestSRTStreamConnection_MissingPort(t *testing.T) {
	result := testStreamConnection("srt://127.0.0.1?streamid=read:test")

	if result.Success {
		t.Fatalf("expected failure for a port-less SRT URL, got success: %+v", result)
	}

	if result.ErrorCode != "NO_PORT" {
		t.Errorf("expected ErrorCode=NO_PORT, got %q (full result: %+v)", result.ErrorCode, result)
	}
}
