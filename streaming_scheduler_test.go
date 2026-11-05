package torrent

import (
	"io"
	"net"
	"testing"
	"time"
	"unique"

	"github.com/anacrolix/torrent/metainfo"
	pp "github.com/anacrolix/torrent/peer_protocol"
)

func TestStreamingPeriodicUpdatePreservesQueue(t *testing.T) {
	p := &PeerConn{}
	p.t = &Torrent{}
	p.initRequestState()
	p.requestState.Interested = true
	p.PeerMaxRequests = 1024
	p.peakRequests = 16
	for i := RequestIndex(0); i < 32; i++ {
		p.requestState.Requests.Add(i)
	}
	before := p.nominalMaxRequests()
	p.applyRequestState(desiredRequestState{Interested: true, Requests: desiredPeerRequests{requestIndexes: []RequestIndex{0}}})
	if p.nominalMaxRequests() != before {
		t.Fatalf("full queue collapsed %d -> %d", before, p.nominalMaxRequests())
	}
}

func newStreamingSchedulerFixture(t *testing.T, pieces int) (*Torrent, []*PeerConn) {
	t.Helper()
	cfg := TestingConfig(t)
	cfg.DefaultStorage = &storageClient{}
	cfg.StreamExclusiveRequests = true
	cfg.DisableTCP, cfg.DisableUTP, cfg.DisablePEX = true, true, true
	cl, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	tor, _ := cl.AddTorrentOpt(testingAddTorrentOpts)
	tor.disableTriggers = true
	if err := tor.setInfoUnlocked(&metainfo.Info{Name: "scheduler", PieceLength: 1 << 20, Length: int64(pieces) << 20, Pieces: make([]byte, pieces*20)}); err != nil {
		t.Fatal(err)
	}
	for i := range pieces {
		tor.pieces[i].priority = PiecePriorityNext
		tor.updatePiecePriorityNoRequests(i)
	}
	var peers []*PeerConn
	for i := range 2 {
		p := cl.newConnection(nil, newConnectionOpts{network: "tcp4", remoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5000 + i}})
		p.setTorrent(tor)
		p.w = io.Discard
		p.initMessageWriter()
		p.onPeerHasAllPiecesNoTriggers()
		p.peerChoking = false
		p.completedHandshake = time.Now()
		tor.conns[p] = struct{}{}
		peers = append(peers, p)
	}
	return tor, peers
}

func TestStreamingExclusiveAssignmentAndQueueRefill(t *testing.T) {
	tor, p := newStreamingSchedulerFixture(t, 8)
	tor.cl.lock()
	defer tor.cl.unlock()
	p[0].stream.rate = 4 << 20
	p[1].stream.rate = 64 << 10
	p[0]._stats.BytesReadUsefulData.Add(1)
	p[1]._stats.BytesReadUsefulData.Add(1)
	tor.scheduleExclusiveStreamingRequests()
	if tor.requestingPeer(0) != p[0] {
		t.Fatal("first block not assigned to the fastest delivery queue")
	}
	if p[0].requestState.Requests.GetCardinality() <= p[1].requestState.Requests.GetCardinality() || p[1].requestState.Requests.IsEmpty() {
		t.Fatal("queues did not reflect measured route speeds")
	}
	owners := make(map[RequestIndex]bool)
	for _, peer := range p {
		for req, count := range peer.validReceiveChunks {
			if owners[req] || count != 1 {
				t.Fatal("overlapping block", req)
			}
			owners[req] = true
		}
	}
	before := tor.streamScheduling.Requests
	tor.scheduleExclusiveStreamingRequests()
	if tor.streamScheduling.Requests != before {
		t.Fatal("periodic pass issued duplicate requests")
	}
	// Refill before the queue empties, not after a full network round-trip gap.
	first := p[0]
	var done []RequestIndex
	first.requestState.Requests.Iterate(func(req RequestIndex) bool {
		done = append(done, req)
		return len(done) < first.streamingRequestLimit()/2+1
	})
	for _, req := range done {
		first.decExpectedChunkReceive(req)
		first.deleteRequest(req)
		tor.dirtyChunks.Add(req)
	}
	if first.requestState.Requests.IsEmpty() || !first.isLowOnRequests() {
		t.Fatal("missing low-watermark refill")
	}
	tor.scheduleExclusiveStreamingRequests()
	if tor.streamScheduling.Requests <= before {
		t.Fatal("did not refill a partly occupied queue")
	}
}

func TestStreamingCancelledBlockRemainsReserved(t *testing.T) {
	for _, fast := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "fast"}[fast], func(t *testing.T) {
			tor, peers := newStreamingSchedulerFixture(t, 1)
			tor.cl.lock()
			defer tor.cl.unlock()
			owner, other := peers[0], peers[1]
			owner.PeerExtensionBytes.SetBit(pp.ExtensionBitFast, fast)
			owner.stream.rate = 4 << 20
			tor.scheduleExclusiveStreamingRequests()
			owner = tor.requestingPeer(0)
			if owner == nil {
				t.Fatal("no owner")
			}
			for _, p := range peers {
				if p != owner {
					other = p
				}
			}
			other.PeerMaxRequests = 1024
			other._stats.BytesReadUsefulData.Add(1)
			other.stream.rate = 4 << 20
			owner.cancel(0)
			owner.peerChoking = true
			tor.scheduleExclusiveStreamingRequests()
			if tor.requestingPeer(0) != nil || other.validReceiveChunks[0] != 0 {
				t.Fatal("reissued a cancelled block before response or connection teardown")
			}
			if fast {
				if !owner.remoteRejectedRequest(0) {
					t.Fatal("reject did not release cancelled request")
				}
			} else {
				owner.close()
				tor.deletePeerConn(owner)
			}
			tor.scheduleExclusiveStreamingRequests()
			if tor.requestingPeer(0) != other {
				t.Fatal("released block was not reassigned")
			}
		})
	}
}

func TestStreamingAdaptiveStallTimeout(t *testing.T) {
	tor, peers := newStreamingSchedulerFixture(t, 1)
	tor.cl.lock()
	defer tor.cl.unlock()
	tor.cl.config.StreamUrgentRequestTimeout = time.Second
	p := peers[0]
	p.stream.rate = 1 << 20
	tor.pieces[0].priority = PiecePriorityNow
	tor.updatePiecePriorityNoRequests(0)
	tor.scheduleExclusiveStreamingRequests()
	owner := tor.requestingPeer(0)
	owner.stream.responseTime = 2 * time.Second
	owner.lastUsefulChunkReceived = time.Now().Add(-3 * time.Second)
	// An elapsed fixed timeout is insufficient while this peer's normal queue
	// latency explains the wait. It must remain the sole owner.
	for req, state := range tor.requestState {
		if state.peer.Value() == owner {
			state.when = time.Now().Add(-3 * time.Second)
			tor.requestState[req] = state
		}
	}
	tor.closeStalledStreamingPeers(peers, time.Now())
	if owner.closed.IsSet() {
		t.Fatal("closed a peer before adaptive timeout")
	}
	owner.lastUsefulChunkReceived = time.Now().Add(-10 * time.Second)
	for req, state := range tor.requestState {
		if state.peer.Value() == owner {
			state.when = time.Now().Add(-10 * time.Second)
			tor.requestState[req] = state
		}
	}
	tor.closeStalledStreamingPeers(peers, time.Now())
	if !owner.closed.IsSet() || tor.streamScheduling.StallCloses != 1 {
		t.Fatal("did not retire stalled owner")
	}
	if tor.requestingPeer(0) != owner {
		t.Fatal("released ownership before connection teardown")
	}
}

func TestStreamingWebseedRangeAvoidsLiveReservations(t *testing.T) {
	tor, peers := newStreamingSchedulerFixture(t, 1)
	tor.cl.lock()
	defer tor.cl.unlock()
	tor.scheduleExclusiveStreamingRequests()
	// The first eight blocks are owned by the two peer queues.
	ws := &webseedPeer{activeRequests: map[*webseedRequest]struct{}{{next: 8, end: 12}: {}}}
	tor.webSeeds[webseedUrlKey(unique.Make("https://example.invalid/video"))] = ws
	peers[0].validReceiveChunks[22] = 1
	begin, end := tor.exclusiveWebseedRange(0, 32)
	if begin != 12 || end != 22 {
		t.Fatalf("unsafe HTTP span [%d,%d)", begin, end)
	}
	// HTTP ownership also excludes simultaneous peer requests.
	peers[0]._stats.BytesReadUsefulData.Add(1)
	peers[0].stream.rate = 1 << 20
	tor.scheduleExclusiveStreamingRequests()
	for req := RequestIndex(8); req < 12; req++ {
		if tor.requestingPeer(req) != nil {
			t.Fatal("peer request overlaps HTTP range", req)
		}
	}
	// The fake webseed was never started; remove it before Client.Close.
	delete(tor.webSeeds, webseedUrlKey(unique.Make("https://example.invalid/video")))
}
