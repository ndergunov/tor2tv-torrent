package torrent

import (
	"context"
	"github.com/anacrolix/chansync"
	"net"
	"testing"
	"time"
)

type identityTestDialer struct{ group string }

func (d identityTestDialer) PeerDialerGroup() string                      { return d.group }
func (identityTestDialer) DialerNetwork() string                          { return "tcp4" }
func (identityTestDialer) Dial(context.Context, string) (net.Conn, error) { panic("not used") }

func TestPeerIdentitySelfIDsAndGroups(t *testing.T) {
	var primary, secondary, remote PeerID
	primary[0], secondary[0], remote[0] = 1, 2, 3
	cl := Client{peerID: primary, config: &ClientConfig{AdditionalPeerIDs: []PeerID{secondary}}}
	if !cl.isOwnPeerID(primary) || !cl.isOwnPeerID(secondary) || cl.isOwnPeerID(remote) {
		t.Fatal("incorrect self-connection detection")
	}
	if dialerGroup(identityTestDialer{"wan"}) != "wan" {
		t.Fatal("lost dialer group")
	}
	if dialerGroup(NetworkDialer{Network: "tcp4", Dialer: &net.Dialer{}}) != "" {
		t.Fatal("default group changed")
	}
}

// A state update may arrive after fillWriteBuffer examines state but before
// the writer begins waiting. The update must trigger another fill immediately.
func TestMessageWriterRetainsWakeDuringFill(t *testing.T) {
	var closed chansync.SetOnce
	progressed := make(chan struct{})
	stopped := make(chan struct{})
	writer := peerConnMsgWriter{closed: &closed, writeBuffer: new(peerConnMsgWriterBuffer), keepAlive: func() bool { return false }}
	calls := 0
	writer.fillWriteBuffer = func() {
		calls++
		if calls == 1 {
			writer.writeCond.Broadcast()
			return
		}
		close(progressed)
		closed.Set()
	}
	go func() { defer close(stopped); writer.run(time.Hour) }()
	defer func() { closed.Set(); <-stopped }()
	select {
	case <-progressed:
	case <-time.After(time.Second):
		t.Fatal("writer lost the state-update wake")
	}
}
