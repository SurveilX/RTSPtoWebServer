package main

import (
	"math"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/datarhei/gosrt"
	"github.com/deepch/vdk/av"
	"github.com/deepch/vdk/format/ts"
	"github.com/sirupsen/logrus"
)

// StreamServerRunStreamSRT is the SRT ingestion path. It is structured identically to
// StreamServerRunStreamRTMP (streamCore.go) — same defer/status/signal shape, same per-packet
// fan-out to HLS and live clients — with SRT's connect-and-demux substituted for RTMP's.
// deepch/vdk's MPEG-TS demuxer (format/ts) already ships a pull-based Streams()/ReadPacket() API
// shaped exactly like the RTMP connection's own, so the two paths share almost their entire body;
// this is deliberate reuse of an already-proven pattern, not a new design.
func StreamServerRunStreamSRT(streamID string, channelID string, opt *ChannelST) (int, error) {
	OutgoingPacketQueue := make(chan *av.Packet, 1000)
	Signals := make(chan int, 100)
	var start bool
	var fps int
	var preKeyTS = time.Duration(0)
	var Seq []*av.Packet

	baseLogger := log.WithFields(logrus.Fields{
		"module":  "core",
		"stream":  streamID,
		"channel": channelID,
		"func":    "StreamServerRunStreamSRT",
	})

	parsedURL, err := url.Parse(opt.URL)
	if err != nil {
		return 0, err
	}

	query := parsedURL.Query()
	srtConfig := srt.DefaultConfig()
	srtConfig.StreamId = query.Get("streamid")
	srtConfig.ConnectionTimeout = 5 * time.Second
	if latencyMs, latencyErr := strconv.Atoi(query.Get("latency")); latencyErr == nil && latencyMs > 0 {
		srtConfig.Latency = time.Duration(latencyMs) * time.Millisecond
	}

	conn, err := srt.Dial("srt", net.JoinHostPort(parsedURL.Hostname(), parsedURL.Port()), srtConfig)
	if err != nil {
		return 0, err
	}

	Storage.StreamChannelStatus(streamID, channelID, ONLINE)
	defer func() {
		// gosrt's Close() is documented to unblock any pending Read — this is what lets the
		// feeder goroutine's blocking ReadPacket() below return promptly whenever this function
		// exits for any reason (keyTest firing, a core signal, a demuxer error), the same way
		// RTMP's conn.Close() unblocks its own feeder goroutine today.
		conn.Close()
		Storage.StreamChannelStatus(streamID, channelID, OFFLINE)
		Storage.StreamHLSFlush(streamID, channelID)
	}()
	var WaitCodec bool

	demuxer := ts.NewDemuxer(conn)

	// Bounded only for this one call. Probing PAT/PMT happens before the keyTest watchdog below
	// has any chance to protect against a peer that completes the SRT handshake but never
	// actually sends anything. Cleared immediately after, so the steady-state read loop behaves
	// like RTMP's — relies on keyTest to detect a stalled source, not a per-read deadline, which
	// would risk killing a healthy connection during a normal brief gap between packets.
	if deadlineErr := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); deadlineErr != nil {
		baseLogger.Warnf("Failed to set initial SRT read deadline: %v", deadlineErr)
	}

	codecs, err := demuxer.Streams()
	if err != nil {
		return 0, err
	}

	if clearErr := conn.SetReadDeadline(time.Time{}); clearErr != nil {
		baseLogger.Warnf("Failed to clear SRT read deadline: %v", clearErr)
	}

	preDur := make([]time.Duration, len(codecs))
	// SRT/MPEG-TS carries no SDP — same empty-SDP precedent already established by the RTMP path.
	Storage.StreamChannelCodecsUpdate(streamID, channelID, codecs, []byte{})

	baseLogger.WithFields(logrus.Fields{"call": "Start"}).Infoln("Success connection SRT")

	// Started here, not at the top of the function: on this codebase's RTSP/RTMP paths, dial is
	// bounded to 3s so it never eats meaningfully into these budgets. SRT's dial + the PAT/PMT
	// probe above have both been observed taking 20-35s on a real, lossy relayed link (Tailscale
	// DERP) - long enough that a timer started before either would already be expired by the time
	// this loop's first iteration runs, killing every connection within ~1-2s of it succeeding
	// regardless of stream health. These timers exist to catch a source that goes quiet AFTER we
	// start actually reading from it, not to time the connection setup itself.
	//
	// srtNoVideoTimeout is 60s, not the RTSP/RTMP paths' 20s, and deliberately so: measured
	// directly against a real SRT source (ffprobe on the raw stream), this link's steady-state
	// keyframe interval is a normal 4s, but the same link was independently observed dropping and
	// corrupting packets badly enough (real "RCV-DROPPED"/decode errors) to lose 5+ consecutive
	// keyframes in a row - a known, already-documented transient property of this Tailscale relay,
	// not a defect in this ingestion path. 60s tolerates that without silently hiding a camera
	// that's actually gone dark - it's still a bounded watchdog, just sized to the link SRT exists
	// to serve rather than to a well-behaved local RTSP feed's assumptions.
	const srtNoVideoTimeout = 60 * time.Second
	keyTest := time.NewTimer(srtNoVideoTimeout)
	checkClients := time.NewTimer(srtNoVideoTimeout)

	var ProbeCount int
	var ProbeFrame int
	var ProbePTS time.Duration
	Storage.NewHLSMuxer(streamID, channelID)
	defer Storage.HLSMuxerClose(streamID, channelID)

	go func() {
		defer func() {
			// This demuxer is exercising a data stream it hasn't seen in production yet —
			// recover rather than let a malformed-MPEG-TS panic take down the whole process.
			if r := recover(); r != nil {
				baseLogger.Errorf("Recovered panic in SRT feeder: %v", r)
			}
		}()
		for {
			pkt, readErr := demuxer.ReadPacket()
			if readErr != nil {
				break
			}
			OutgoingPacketQueue <- &pkt
		}
		Signals <- 1
	}()

	for {
		select {
		// Check stream has clients
		case <-checkClients.C:
			if opt.OnDemand && !Storage.ClientHas(streamID, channelID) {
				return 1, ErrorStreamNoClients
			}
			checkClients.Reset(srtNoVideoTimeout)
		// Check stream sends key
		case <-keyTest.C:
			return 0, ErrorStreamNoVideo
		// Read core signals
		case signals := <-opt.signals:
			switch signals {
			case SignalStreamStop:
				return 2, ErrorStreamStopCoreSignal
			case SignalStreamRestart:
				return 0, ErrorStreamRestart
			case SignalStreamClient:
				return 1, ErrorStreamNoClients
			}
		// Feeder goroutine exited (demuxer read error)
		case <-Signals:
			return 0, ErrorStreamStopRTSPSignal
		case packetAV := <-OutgoingPacketQueue:
			if packetAV.Idx >= 0 && int(packetAV.Idx) < len(preDur) {
				if preDur[packetAV.Idx] != 0 {
					packetAV.Duration = packetAV.Time - preDur[packetAV.Idx]
				}
				preDur[packetAV.Idx] = packetAV.Time
			}

			if WaitCodec {
				continue
			}

			if packetAV.IsKeyFrame {
				keyTest.Reset(srtNoVideoTimeout)
				if preKeyTS > 0 {
					Storage.StreamHLSAdd(streamID, channelID, Seq, packetAV.Time-preKeyTS)
					Seq = []*av.Packet{}
				}
				preKeyTS = packetAV.Time
			}
			Seq = append(Seq, packetAV)
			Storage.StreamChannelCast(streamID, channelID, packetAV)
			/*
			   HLS LL Test
			*/
			if packetAV.IsKeyFrame && !start {
				start = true
			}
			/*
				FPS mode probe
			*/
			if start {
				ProbePTS += packetAV.Duration
				ProbeFrame++
				if packetAV.IsKeyFrame && ProbePTS.Seconds() >= 1 {
					ProbeCount++
					if ProbeCount == 2 {
						fps = int(math.Round(float64(ProbeFrame) / ProbePTS.Seconds()))
					}
					ProbeFrame = 0
					ProbePTS = 0
				}
			}
			if start && fps != 0 {
				//TODO fix it
				packetAV.Duration = time.Duration((float32(1000)/float32(fps))*1000*1000) * time.Nanosecond
				Storage.HlsMuxerSetFPS(streamID, channelID, fps)
				Storage.HlsMuxerWritePacket(streamID, channelID, packetAV)
			}
		}
	}
}
