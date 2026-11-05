package torrent

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

type pathFailureDialer struct{ calls chan string }

func (d pathFailureDialer) DialerNetwork() string   { return "tcp4" }
func (d pathFailureDialer) PeerDialerGroup() string { return "wan" }
func (d pathFailureDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	d.calls <- addr
	return nil, errors.New("synthetic route unavailable")
}

func TestStreamingMissingIdentityRetriesWithoutRediscovery(t *testing.T) {
	for _, incoming := range []bool{false, true} {
		t.Run(map[bool]string{false: "outgoing", true: "incoming-advertised-port"}[incoming], func(t *testing.T) {
			tor, peers := newStreamingSchedulerFixture(t, 2)
			calls := make(chan string, 8)
			tor.cl.AddDialer(pathFailureDialer{calls})
			tor.cl.lock()
			tor.cl.config.PeerDialerGroups = func(PeerRemoteAddr) []string { return []string{"vpn", "wan"} }
			primary := peers[0]
			primary.localIdentity.DialerGroup = "vpn"
			primary.outgoing = !incoming
			want := primary.RemoteAddr.String()
			if incoming {
				primary.PeerListenPort = 61234
				want = "127.0.0.1:61234"
			}
			peers[1].close()
			tor.deletePeerConn(peers[1])
			at := time.Now()
			tor.maintainPeerPaths(at)
			tor.cl.unlock()
			select {
			case addr := <-calls:
				if addr != want {
					t.Fatalf("dialed %s, want %s", addr, want)
				}
			case <-time.After(time.Second):
				t.Fatal("no missing-route dial")
			}
			// Wait for the failed attempt's accounting to finish.
			deadline := time.Now().Add(time.Second)
			for {
				tor.cl.lock()
				pending := tor.numHalfOpenAttempts()
				tor.cl.unlock()
				if pending == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("attempt stuck")
				}
				time.Sleep(time.Millisecond)
			}
			tor.cl.lock()
			state := tor.peerPaths[peerPathKey{want, "wan"}]
			if state == nil || state.attempts != 1 || state.failures != 1 || state.lastError == "" {
				t.Fatal("missing failure diagnostics", state)
			}
			tor.maintainPeerPaths(at.Add(2 * time.Second))
			if state.attempts != 1 {
				t.Fatal("ignored retry backoff")
			}
			tor.maintainPeerPaths(at.Add(6 * time.Second))
			if state.attempts != 2 {
				t.Fatal("lost secondary path after a failed first attempt")
			}
			tor.cl.unlock()
			select {
			case <-calls:
			case <-time.After(time.Second):
				t.Fatal("no secondary retry")
			}
		})
	}
}
func TestStreamingRouteCountersSurviveClosedSession(t *testing.T) {
	tor, peers := newStreamingSchedulerFixture(t, 1)
	tor.cl.lock()
	defer tor.cl.unlock()
	p := peers[0]
	p.localIdentity.DialerGroup = "wan"
	p._stats.BytesReadUsefulData.Add(123)
	p._stats.BytesReadData.Add(130)
	p.close()
	tor.deletePeerConn(p)
	_, routes := tor.streamingPaths(time.Now())
	for _, s := range routes {
		if s.Route == "wan" {
			if s.UsefulBytes != 123 || s.DataBytes != 130 || s.Sessions != 0 {
				t.Fatal(s)
			}
			return
		}
	}
	t.Fatal("closed route counters lost")
}

type pathDHTStub struct {
	DhtServer
	port int
}

func (s pathDHTStub) Addr() net.Addr { return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: s.port} }
func TestStreamingDHTPortFollowsLocalIdentity(t *testing.T) {
	cl := &Client{dhtServers: []DhtServer{pathDHTStub{port: 6881}, pathDHTStub{port: 6882}}}
	p := &PeerConn{}
	p.t = &Torrent{cl: cl}
	for _, port := range []int{6881, 6882} {
		p.localIdentity.ListenPort = port
		if int(p.dhtPort()) != port {
			t.Fatalf("DHT port does not follow local identity %d", port)
		}
	}
}
