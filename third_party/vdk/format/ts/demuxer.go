package ts

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/deepch/vdk/av"
	"github.com/deepch/vdk/codec/aacparser"
	"github.com/deepch/vdk/codec/h264parser"
	"github.com/deepch/vdk/codec/h265parser"
	"github.com/deepch/vdk/codec/mjpeg"
	"github.com/deepch/vdk/format/ts/tsio"
	"github.com/deepch/vdk/utils/bits/pio"
)

type Demuxer struct {
	r *bufio.Reader

	pkts []av.Packet

	pat     *tsio.PAT
	pmt     *tsio.PMT
	streams []*Stream
	tshdr   []byte
	AnnexB  bool
	stage   int
}

func NewDemuxer(r io.Reader) *Demuxer {
	return &Demuxer{
		tshdr: make([]byte, 188),
		r:     bufio.NewReaderSize(r, pio.RecommendBufioSize),
	}
}

func (self *Demuxer) Streams() (streams []av.CodecData, err error) {
	if err = self.probe(); err != nil {
		return
	}
	for _, stream := range self.streams {
		streams = append(streams, stream.CodecData)
	}
	return
}

func (self *Demuxer) probe() (err error) {
	if self.stage == 0 {
		for {
			if self.pmt != nil {
				n := 0
				for _, stream := range self.streams {
					if stream.CodecData != nil {
						n++
					}
				}
				if n == len(self.streams) {
					break
				}
			}
			if err = self.poll(); err != nil {
				return
			}
		}
		self.stage++
	}
	return
}

func (self *Demuxer) ReadPacket() (pkt av.Packet, err error) {
	if err = self.probe(); err != nil {
		return
	}

	for len(self.pkts) == 0 {
		if err = self.poll(); err != nil {
			return
		}
	}

	pkt = self.pkts[0]
	self.pkts = self.pkts[1:]
	return
}

func (self *Demuxer) poll() (err error) {
	if err = self.readTSPacket(); err == io.EOF {
		var n int
		if n, err = self.payloadEnd(); err != nil {
			return
		}
		if n == 0 {
			err = io.EOF
		}
	}
	return
}

func (self *Demuxer) initPMT(payload []byte) (err error) {
	var psihdrlen int
	var datalen int
	if _, _, psihdrlen, datalen, err = tsio.ParsePSI(payload); err != nil {
		return
	}
	self.pmt = &tsio.PMT{}
	if _, err = self.pmt.Unmarshal(payload[psihdrlen : psihdrlen+datalen]); err != nil {
		return
	}

	self.streams = []*Stream{}
	for i, info := range self.pmt.ElementaryStreamInfos {
		stream := &Stream{}
		stream.idx = i
		stream.demuxer = self
		stream.pid = info.ElementaryPID
		stream.streamType = info.StreamType
		switch info.StreamType {
		case tsio.ElementaryStreamTypeH264:
			self.streams = append(self.streams, stream)
		// H265/HEVC (stream_type 0x24) was never wired in here even though the constant already
		// existed in tsio - any HEVC elementary stream was silently dropped at PMT-parse time,
		// before a single packet was ever read. Confirmed live against a real HEVC-over-SRT
		// source: the demuxer connected successfully (PAT/PMT parse fine) but produced zero
		// packets forever, because the video PID was never added to self.streams at all.
		case tsio.ElementaryStreamTypeH265:
			self.streams = append(self.streams, stream)
		case tsio.ElementaryStreamTypeAdtsAAC:
			self.streams = append(self.streams, stream)
		case tsio.ElementaryStreamTypeAlignmentDescriptor:
			self.streams = append(self.streams, stream)
		}
	}
	return
}

func (self *Demuxer) payloadEnd() (n int, err error) {
	for _, stream := range self.streams {
		var i int
		if i, err = stream.payloadEnd(); err != nil {
			return
		}
		n += i
	}
	return
}

func (self *Demuxer) readTSPacket() (err error) {
	var hdrlen int
	var pid uint16
	var start bool
	var iskeyframe bool

	if _, err = io.ReadFull(self.r, self.tshdr); err != nil {
		return
	}

	if pid, start, iskeyframe, hdrlen, err = tsio.ParseTSHeader(self.tshdr); err != nil {
		return
	}
	payload := self.tshdr[hdrlen:]

	if self.pat == nil {
		if pid == 0 {
			var psihdrlen int
			var datalen int
			if _, _, psihdrlen, datalen, err = tsio.ParsePSI(payload); err != nil {
				return
			}
			self.pat = &tsio.PAT{}
			if _, err = self.pat.Unmarshal(payload[psihdrlen : psihdrlen+datalen]); err != nil {
				return
			}
		}
	} else if self.pmt == nil {
		for _, entry := range self.pat.Entries {
			if entry.ProgramMapPID == pid {
				if err = self.initPMT(payload); err != nil {
					return
				}
				break
			}
		}
	} else {
		for _, stream := range self.streams {
			if pid == stream.pid {
				if stream.streamType == tsio.ElementaryStreamTypeAdtsAAC {
					iskeyframe = false
				}
				if err = stream.handleTSPacket(start, iskeyframe, payload); err != nil {
					return
				}
				break
			}
		}
	}

	return
}

func (self *Stream) addPacket(payload []byte, timedelta time.Duration, fixed time.Duration) {
	dts := self.dts
	pts := self.pts

	if dts == 0 {
		dts = pts
	}

	dur := time.Duration(0)

	if self.pt > 0 {
		dur = dts + timedelta - self.pt
	} else {
		dur = fixed
	}

	self.pt = dts + timedelta

	demuxer := self.demuxer
	pkt := av.Packet{
		Idx:        int8(self.idx),
		IsKeyFrame: self.iskeyframe,
		Time:       dts + timedelta,
		Data:       payload,
		Duration:   dur,
	}
	if pts != dts {
		pkt.CompositionTime = pts - dts
	}
	demuxer.pkts = append(demuxer.pkts, pkt)
}

func (self *Stream) payloadEnd() (n int, err error) {
	payload := self.data
	if payload == nil {
		return
	}
	if self.datalen != 0 && len(payload) != self.datalen {
		err = fmt.Errorf("ts: packet size mismatch size=%d correct=%d", len(payload), self.datalen)
		return
	}
	self.data = nil
	switch self.streamType {
	case tsio.ElementaryStreamTypeAlignmentDescriptor:
		if self.CodecData == nil {
			self.CodecData = mjpeg.CodecData{}
		}
		b := make([]byte, 4+len(payload))
		pio.PutU32BE(b[0:4], uint32(len(payload)))
		copy(b[4:], payload)
		self.addPacket(b, time.Duration(0), 0)
		n++
	case tsio.ElementaryStreamTypeAdtsAAC:
		var config aacparser.MPEG4AudioConfig

		delta := time.Duration(0)
		for len(payload) > 0 {
			var hdrlen, framelen, samples int
			if config, hdrlen, framelen, samples, err = aacparser.ParseADTSHeader(payload); err != nil {
				return
			}
			if self.CodecData == nil {
				if self.CodecData, err = aacparser.NewCodecDataFromMPEG4AudioConfig(config); err != nil {
					return
				}
			}
			self.addPacket(payload[hdrlen:framelen], delta, time.Duration(samples)*time.Second/time.Duration(config.SampleRate))
			n++
			delta += time.Duration(samples) * time.Second / time.Duration(config.SampleRate)
			payload = payload[framelen:]
		}

	case tsio.ElementaryStreamTypeH264:
		nalus, _ := h264parser.SplitNALUs(payload)
		var sps, pps []byte

		for _, nalu := range nalus {
			if len(nalu) > 0 {
				naltype := nalu[0] & 0x1f
				switch {
				case naltype == 7:
					sps = nalu
					info, err := h264parser.ParseSPS(sps)
					if err == nil {
						self.fps = info.FPS
					}
				case naltype == 8:
					pps = nalu
				case h264parser.IsDataNALU(nalu):
					// raw nalu to avcc
					if !self.demuxer.AnnexB {
						b := make([]byte, 4+len(nalu))
						pio.PutU32BE(b[0:4], uint32(len(nalu)))
						copy(b[4:], nalu)
						fps := self.fps
						if self.fps == 0 {
							fps = 25
						}
						self.addPacket(b, time.Duration(0), (1000*time.Millisecond)/time.Duration(fps))
						n++
					}
				}
			}
		}

		if self.demuxer.AnnexB {
			b := make([]byte, 4+len(payload))
			pio.PutU32BE(b[0:4], uint32(len(payload)))
			copy(b[4:], payload)
			self.addPacket(b, time.Duration(0), 0)
			n++
		}

		if self.CodecData == nil && len(sps) > 0 && len(pps) > 0 {
			if self.CodecData, err = h264parser.NewCodecDataFromSPSAndPPS(sps, pps); err != nil {
				return
			}
		}

	case tsio.ElementaryStreamTypeH265:
		nalus, _ := h265parser.SplitNALUs(payload)
		var vps, sps, pps []byte

		for _, nalu := range nalus {
			if len(nalu) > 1 {
				// HEVC's NAL header is 2 bytes, not H264's 1 - the type is bits 1-6 of the
				// first byte. Deliberately not using h265parser.IsDataNALU here: despite its
				// name, that function still applies H264's 1-byte/5-bit formula, so it
				// misclassifies every HEVC NAL unit. VCL (coded-slice) types are 0-31;
				// everything above that (including VPS/SPS/PPS at 32/33/34) is non-VCL.
				naltype := (nalu[0] >> 1) & 0x3f
				switch naltype {
				case h265parser.NAL_UNIT_VPS:
					vps = nalu
				case h265parser.NAL_UNIT_SPS:
					sps = nalu
				case h265parser.NAL_UNIT_PPS:
					pps = nalu
				default:
					if naltype <= h265parser.NAL_UNIT_RESERVED_VCL31 {
						// raw nalu to avcc
						if !self.demuxer.AnnexB {
							b := make([]byte, 4+len(nalu))
							pio.PutU32BE(b[0:4], uint32(len(nalu)))
							copy(b[4:], nalu)
							fps := self.fps
							if fps == 0 {
								fps = 25
							}
							self.addPacket(b, time.Duration(0), (1000*time.Millisecond)/time.Duration(fps))
							n++
						}
					}
				}
			}
		}

		if self.demuxer.AnnexB {
			b := make([]byte, 4+len(payload))
			pio.PutU32BE(b[0:4], uint32(len(payload)))
			copy(b[4:], payload)
			self.addPacket(b, time.Duration(0), 0)
			n++
		}

		// HEVC needs all three parameter-set types (H264 only needs SPS+PPS) - and unlike
		// h264parser.SPSInfo, h265parser.SPSInfo's fps field is unexported, so FPS can only be
		// read back via CodecData.FPS() once the full VPS+SPS+PPS record has been built, not
		// from a bare parsed SPS the way the H264 branch above does.
		if self.CodecData == nil && len(vps) > 0 && len(sps) > 0 && len(pps) > 0 {
			codecData, cerr := h265parser.NewCodecDataFromVPSAndSPSAndPPS(vps, sps, pps)
			if cerr != nil {
				err = cerr
				return
			}
			self.CodecData = codecData
			if fps := codecData.FPS(); fps > 0 {
				self.fps = uint(fps)
			}
		}
	}

	return
}

func (self *Stream) handleTSPacket(start bool, iskeyframe bool, payload []byte) (err error) {
	if start {
		if _, err = self.payloadEnd(); err != nil {
			return
		}
		var hdrlen int
		if hdrlen, _, self.datalen, self.pts, self.dts, err = tsio.ParsePESHeader(payload); err != nil {
			return
		}
		self.iskeyframe = iskeyframe
		if self.datalen == 0 {
			self.data = make([]byte, 0, 4096)
		} else {
			self.data = make([]byte, 0, self.datalen)
		}
		self.data = append(self.data, payload[hdrlen:]...)
	} else {
		self.data = append(self.data, payload...)
	}
	return
}
