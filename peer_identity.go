package torrent

// PeerIdentity describes the local peer-wire identity of a connection. Tracker
// and DHT discovery retain Client.PeerID and Client.LocalPort. Each identity
// shares the client's torrent, scheduler, storage, limits and tracker counters.
type PeerIdentity struct {
	PeerID      PeerID
	ListenPort  int
	DialerGroup string
}

// LocalPeerIdentity returns an immutable snapshot selected before handshake.
func (pc *PeerConn) LocalPeerIdentity() PeerIdentity { return pc.localIdentity }

func dialerGroup(d Dialer) string {
	if tagged, ok := d.(interface{ PeerDialerGroup() string }); ok {
		return tagged.PeerDialerGroup()
	}
	return ""
}

func (cl *Client) isOwnPeerID(id PeerID) bool {
	if id == cl.peerID {
		return true
	}
	for _, own := range cl.config.AdditionalPeerIDs {
		if id == own {
			return true
		}
	}
	return false
}
