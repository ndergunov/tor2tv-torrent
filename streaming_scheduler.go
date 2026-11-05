package torrent

import (
	"math"
	"slices"
	"time"

	requestStrategy "github.com/anacrolix/torrent/internal/request-strategy"
	"github.com/anacrolix/torrent/metainfo"
	typedRoaring "github.com/anacrolix/torrent/typed-roaring"
)

// All scheduler state is protected by the Client lock. There is one owner per
// block across every local identity, including requests cancelled on a live
// connection: cancel does not recall a Piece already in flight.
type streamPeerState struct {
	cancelledAt       map[RequestIndex]time.Time
	rate              float64
	sampledAt         time.Time
	sampledBytes      int64
	sampledActive     bool
	responseTime      time.Duration
	requests, cancels int64
}

type streamSchedulerCounters struct {
	Requests    int64 `json:"requestsIssued"`
	Cancels     int64 `json:"cancelsIssued"`
	StallCloses int64 `json:"stallCloses"`
}

type StreamingPeerStatus struct {
	Pending           int     `json:"pendingBlocks"`
	Cancelled         int     `json:"pendingCancels"`
	Limit             int     `json:"requestLimit"`
	Rate              float64 `json:"estimatedBytesPerSecond"`
	QueueSeconds      float64 `json:"queueSeconds"`
	OldestSeconds     float64 `json:"oldestRequestSeconds"`
	LastUsefulSeconds float64 `json:"lastUsefulSeconds"`
	ResponseSeconds   float64 `json:"responseSeconds"`
	Choked            bool    `json:"choked"`
	Requests          int64   `json:"requestsIssued"`
	Cancels           int64   `json:"cancelsIssued"`
}

type StreamingReaderStatus struct {
	Position          int64 `json:"positionBytes"`
	Contiguous        int64 `json:"contiguousBytes"`
	Window            int64 `json:"windowBytes"`
	FirstMissingPiece int   `json:"firstMissingPiece"`
}

type StreamingStatus struct {
	Exclusive bool `json:"exclusive"`
	streamSchedulerCounters
	Pending         int                     `json:"pendingBlocks"`
	DuplicateBlocks int                     `json:"overlappingBlocks"`
	Readers         []StreamingReaderStatus `json:"readers"`
}

func (p *PeerConn) streamingRate() float64 {
	if p.stream.rate > 0 {
		return p.stream.rate
	}
	if rate := p.downloadRate(); rate > 0 && !math.IsInf(rate, 0) && !math.IsNaN(rate) {
		return rate
	}
	return 128 << 10 // Limited bootstrap probes, not a claim of measured capacity.
}

func (p *PeerConn) sampleStreamingRate(now time.Time) {
	s := &p.stream
	total := p._stats.BytesReadUsefulData.Int64()
	active := !p.requestState.Requests.IsEmpty()
	if s.sampledAt.IsZero() {
		s.sampledAt, s.sampledBytes, s.sampledActive = now, total, active
		return
	}
	dt := now.Sub(s.sampledAt).Seconds()
	if dt < .25 {
		return
	}
	if s.sampledActive || active {
		sample := float64(total-s.sampledBytes) / dt
		if s.rate == 0 {
			s.rate = sample
		} else {
			s.rate += (1 - math.Exp(-dt/2)) * (sample - s.rate)
		}
	}
	s.sampledAt, s.sampledBytes, s.sampledActive = now, total, active
}

func (p *PeerConn) streamingRequestLimit() int {
	limit := 4 // A new peer must prove its speed before receiving a large queue.
	if p._stats.BytesReadUsefulData.Int64() > 0 {
		limit = max(4, int(math.Ceil(p.streamingRate()*1.5/float64(p.t.chunkSize)))+2)
	}
	return max(1, min(limit, p.PeerMaxRequests, maxLocalToRemoteRequests))
}

func (p *PeerConn) recordStreamingResponse(req RequestIndex, now time.Time) {
	if !p.t.cl.config.StreamExclusiveRequests {
		return
	}
	state, ok := p.t.requestState[req]
	if !ok || state.peer.Value() != p {
		return
	}
	elapsed := now.Sub(state.when)
	if p.stream.responseTime == 0 {
		p.stream.responseTime = elapsed
	} else {
		p.stream.responseTime += (elapsed - p.stream.responseTime) / 8
	}
}

func (p *PeerConn) streamingPeerStatus(now time.Time) StreamingPeerStatus {
	out := StreamingPeerStatus{Pending: int(p.requestState.Requests.GetCardinality()), Cancelled: int(p.requestState.Cancelled.GetCardinality()), Limit: p.nominalMaxRequests(), Rate: p.streamingRate(), ResponseSeconds: p.stream.responseTime.Seconds(), Choked: p.peerChoking, Requests: p.stream.requests, Cancels: p.stream.cancels}
	out.QueueSeconds = float64(out.Pending) * float64(p.t.chunkSize) / out.Rate
	if !p.lastUsefulChunkReceived.IsZero() {
		out.LastUsefulSeconds = now.Sub(p.lastUsefulChunkReceived).Seconds()
	}
	p.requestState.Requests.Iterate(func(req RequestIndex) bool {
		out.OldestSeconds = max(out.OldestSeconds, now.Sub(p.t.requestState[req].when).Seconds())
		return true
	})
	return out
}

// StreamingDiagnostics is a consistent snapshot, including the contiguous
// available range at each reader. Total cache usage is not a buffer metric.
func (t *Torrent) StreamingDiagnostics() StreamingStatus {
	t.cl.rLock()
	defer t.cl.rUnlock()
	out := StreamingStatus{Exclusive: t.cl.config.StreamExclusiveRequests, streamSchedulerCounters: t.streamScheduling, Pending: len(t.requestState)}
	copies := make(map[RequestIndex]int)
	for pc := range t.conns {
		if pc.closed.IsSet() {
			continue
		}
		for req, count := range pc.validReceiveChunks {
			copies[req] += count
		}
	}
	for _, count := range copies {
		if count > 1 {
			out.DuplicateBlocks++
		}
	}
	if t.haveInfo() {
		for r := range t.readers {
			if !r.reading {
				continue
			}
			window := min(r.length-r.pos, int64(r.pieces.end)*t.info.PieceLength-r.offset-r.pos)
			window = max(0, window)
			contiguous := r.available(r.pos, window)
			missing := -1
			if contiguous < window {
				missing = int((r.offset + r.pos + contiguous) / t.info.PieceLength)
			}
			out.Readers = append(out.Readers, StreamingReaderStatus{Position: r.offset + r.pos, Contiguous: contiguous, Window: window, FirstMissingPiece: missing})
		}
	}
	return out
}

type streamBlock struct {
	index    RequestIndex
	piece    int
	priority PiecePriority
}

func (t *Torrent) scheduleExclusiveStreamingRequests() {
	if !t.haveInfo() || t.closed.IsSet() || t.dataDownloadDisallowed.Bool() {
		return
	}
	now := time.Now()
	peers := make([]*PeerConn, 0, len(t.conns))
	for p := range t.conns {
		if !p.closed.IsSet() {
			p.sampleStreamingRate(now)
			peers = append(peers, p)
		}
	}
	// Stable tie-breaking avoids map iteration deciding the initial probes.
	slices.SortFunc(peers, func(a, b *PeerConn) int {
		if a.completedHandshake.Before(b.completedHandshake) {
			return -1
		}
		if a.completedHandshake.After(b.completedHandshake) {
			return 1
		}
		return 0
	})
	t.closeStalledStreamingPeers(peers, now)
	reserved := make(map[RequestIndex]bool)
	for p := range t.conns {
		// A closed connection retains ownership until the read loop has exited and
		// deletePeerConn releases its requests. Do not overlap even during teardown.
		for req := range p.validReceiveChunks {
			reserved[req] = true
		}
	}
	for req := range t.requestState {
		reserved[req] = true
	}
	for _, ws := range t.webSeeds {
		for wr := range ws.activeRequests {
			for req := wr.next; req < wr.end; req++ {
				reserved[req] = true
			}
		}
	}
	blocks := make([]streamBlock, 0)
	interested := make(map[*PeerConn]bool, len(peers))
	var it typedRoaring.Iterator[RequestIndex]
	t.getRequestablePieces(func(ih metainfo.Hash, piece int, state requestStrategy.PieceRequestOrderState) bool {
		if ih != *t.canonicalShortInfohash() {
			return true
		}
		for _, p := range peers {
			if !p.closed.IsSet() && p.peerHasPiece(piece) && !p.peerAllowedFast.Contains(piece) {
				interested[p] = true
			}
		}
		t.iterUndirtiedRequestIndexesInPiece(&it, piece, func(req RequestIndex) {
			if !reserved[req] {
				blocks = append(blocks, streamBlock{req, piece, state.Priority})
			}
		})
		return true
	})
	for _, p := range peers {
		if p.closed.IsSet() {
			continue
		}
		want := interested[p] || (p.requestState.Interested && !p.allowSendNotInterested())
		p.setInterested(want)
	}
	slices.SortFunc(blocks, func(a, b streamBlock) int {
		if a.priority != b.priority {
			return int(b.priority) - int(a.priority)
		}
		if a.index < b.index {
			return -1
		}
		if a.index > b.index {
			return 1
		}
		return 0
	})
	for _, block := range blocks {
		var best *PeerConn
		bestTime := math.Inf(1)
		// Keep known working peers on the current head; test unknown peers on
		// subsequent pieces when there is a measured alternative.
		knownAlternative := false
		if block.priority == PiecePriorityNow {
			for _, p := range peers {
				if !p.closed.IsSet() && !p.peerChoking && p.peerHasPiece(block.piece) && p._stats.BytesReadUsefulData.Int64() > 0 {
					knownAlternative = true
					break
				}
			}
		}
		for _, p := range peers {
			if p.closed.IsSet() || !p.peerHasPiece(block.piece) || (p.peerChoking && !p.peerAllowedFast.Contains(block.piece)) {
				continue
			}
			if p.needRequestUpdate == peerUpdateRequestsRemoteRejectReason {
				continue
			}
			pending := int(p.requestState.Requests.GetCardinality() + p.requestState.Cancelled.GetCardinality())
			if pending >= p.streamingRequestLimit() {
				continue
			}
			if knownAlternative && p._stats.BytesReadUsefulData.Int64() == 0 {
				continue
			}
			delivery := float64(pending+1) * float64(t.chunkSize) / p.streamingRate()
			// Existing queue time is part of the estimate. Each assignment immediately
			// changes the ordering, so both routes receive distinct blocks proportionally.
			if delivery < bestTime {
				best, bestTime = p, delivery
			}
		}
		if best == nil {
			continue
		}
		if !best.mustRequest(block.index) {
			break
		} // Respect writer backpressure.
	}
}

func (t *Torrent) closeStalledStreamingPeers(peers []*PeerConn, now time.Time) {
	floor := t.cl.config.StreamUrgentRequestTimeout
	if floor <= 0 {
		return
	}
	for _, p := range peers {
		if p.closed.IsSet() || (p.requestState.Requests.IsEmpty() && len(p.stream.cancelledAt) == 0) {
			continue
		}
		timeout := max(floor, 4*p.stream.responseTime)
		// A missing head on a dead connection must not survive indefinitely.
		timeout = min(timeout, max(floor, 30*time.Second))
		progress := p.lastUsefulChunkReceived
		if progress.IsZero() {
			progress = p.completedHandshake
		}
		stalled := false
		p.requestState.Requests.Iterate(func(req RequestIndex) bool {
			age := now.Sub(t.requestState[req].when)
			if t.piece(t.pieceIndexOfRequestIndex(req)).purePriority() != PiecePriorityNow || age < timeout || (now.Sub(progress) < timeout && age < max(3*timeout, 30*time.Second)) {
				return true
			}
			for _, other := range peers {
				if other != p && !other.closed.IsSet() && !other.peerChoking && other.peerHasPiece(t.pieceIndexOfRequestIndex(req)) {
					stalled = true
					return false
				}
			}
			return true
		})

		// Non-fast peers do not acknowledge CANCEL. If a seek returns to a
		// cancelled block, retain its reservation until Piece or teardown.
		if !stalled {
			for req, when := range p.stream.cancelledAt {
				if now.Sub(when) < timeout || t.piece(t.pieceIndexOfRequestIndex(req)).purePriority() != PiecePriorityNow {
					continue
				}
				for _, other := range peers {
					if other != p && !other.closed.IsSet() && !other.peerChoking && other.peerHasPiece(t.pieceIndexOfRequestIndex(req)) {
						stalled = true
						break
					}
				}
				if stalled {
					break
				}
			}
		}
		if stalled {
			t.streamScheduling.StallCloses++
			p.logger.Printf("closing stalled streaming connection after %s without useful data; pending=%d", now.Sub(progress), p.requestState.Requests.GetCardinality())
			p.close()
		}
	}
}

// Return only a contiguous unowned span, including across multiple webseeds.
// Live cancelled HTTP ranges remain reserved until their worker exits.
func (t *Torrent) exclusiveWebseedRange(begin, end RequestIndex) (RequestIndex, RequestIndex) {
	reserved := func(req RequestIndex) bool {
		if t.haveChunk(t.requestIndexToRequest(req)) {
			return true
		}
		for p := range t.conns {
			if p.validReceiveChunks[req] > 0 {
				return true
			}
		}
		for _, ws := range t.webSeeds {
			for wr := range ws.activeRequests {
				if req >= wr.next && req < wr.end {
					return true
				}
			}
		}
		return false
	}
	for begin < end && reserved(begin) {
		begin++
	}
	for req := begin; req < end; req++ {
		if reserved(req) {
			return begin, req
		}
	}
	return begin, end
}
