package torrent

import (
	"context"
	"net"
	"testing"
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
