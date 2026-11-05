package torrent

import "time"

type peerPathKey struct{ endpoint, route string }
type peerPathState struct {
	attempts, failures   int64
	lastAttempt, retryAt time.Time
	backoff              time.Duration
	lastError            string
}

type StreamingPathStatus struct {
	Endpoint     string  `json:"endpoint"`
	Route        string  `json:"route"`
	Attempts     int64   `json:"attempts"`
	Failures     int64   `json:"failures"`
	LastError    string  `json:"lastError,omitempty"`
	RetrySeconds float64 `json:"retrySeconds"`
	Connected    bool    `json:"connected"`
}
type StreamingRouteStatus struct {
	Route       string `json:"route"`
	UsefulBytes int64  `json:"usefulBytes"`
	DataBytes   int64  `json:"dataBytes"`
	Sessions    int    `json:"sessions"`
}

// Called under the client lock. Discovery normally consumes a candidate once;
// successful primary sessions and incoming peers must also retain a chance to
// establish a missing independent identity after an initial dial failure.
func (t *Torrent) maintainPeerPaths(now time.Time) {
	groups := t.cl.config.PeerDialerGroups
	if groups == nil || len(t.cl.dialers) == 0 || t.closed.IsSet() || !t.wantOutgoingConns() || now.Sub(t.lastPeerPathCheck) < time.Second {
		return
	}
	t.lastPeerPathCheck = now
	for c := range t.conns {
		if c.closed.IsSet() || c.completedHandshake.IsZero() || (!c.outgoing && c.PeerListenPort == 0) {
			continue
		}
		addr := c.dialAddr()
		for _, group := range groups(addr) {
			if t.hasPeerPath(addr.String(), group) {
				continue
			}
			state := t.peerPaths[peerPathKey{addr.String(), group}]
			if state != nil && now.Before(state.retryAt) {
				continue
			}
			initiateConn(outgoingConnOpts{t: t, peerInfo: PeerInfo{Addr: addr, Source: PeerSourcePex, SupportsEncryption: c.headerEncrypted}, groupSelected: true, dialerGroup: group, HeaderObfuscationPolicy: t.cl.config.HeaderObfuscationPolicy}, false)
		}
	}
}
func (t *Torrent) hasPeerPath(endpoint, route string) bool {
	for c := range t.conns {
		if !c.closed.IsSet() && c.localIdentity.DialerGroup == route && c.dialAddr().String() == endpoint {
			return true
		}
	}
	return false
}
func (t *Torrent) recordPeerPathAttempt(endpoint, route string, now time.Time) {
	if t.peerPaths == nil {
		t.peerPaths = make(map[peerPathKey]*peerPathState)
	}
	key := peerPathKey{endpoint, route}
	state := t.peerPaths[key]
	if state == nil {
		state = &peerPathState{}
		t.peerPaths[key] = state
	}
	state.attempts++
	state.lastAttempt = now
	state.backoff = min(max(5*time.Second, 2*state.backoff), 2*time.Minute)
	state.retryAt = now.Add(state.backoff)
}
func (t *Torrent) recordPeerPathResult(endpoint, route string, err error) {
	state := t.peerPaths[peerPathKey{endpoint, route}]
	if state == nil {
		return
	}
	if err != nil {
		state.failures++
		state.lastError = err.Error()
	} else {
		state.lastError = ""
		state.backoff = 0
	}
}
func (t *Torrent) closedPeerRoute(c *PeerConn) {
	if t.closedRouteStats == nil {
		t.closedRouteStats = make(map[string]StreamingRouteStatus)
	}
	route := c.localIdentity.DialerGroup
	s := t.closedRouteStats[route]
	s.Route = route
	s.UsefulBytes += c._stats.BytesReadUsefulData.Int64()
	s.DataBytes += c._stats.BytesReadData.Int64()
	t.closedRouteStats[route] = s
}
func (t *Torrent) streamingPaths(now time.Time) ([]StreamingPathStatus, []StreamingRouteStatus) {
	var paths []StreamingPathStatus
	for key, state := range t.peerPaths {
		paths = append(paths, StreamingPathStatus{Endpoint: key.endpoint, Route: key.route, Attempts: state.attempts, Failures: state.failures, LastError: state.lastError, RetrySeconds: max(0, state.retryAt.Sub(now).Seconds()), Connected: t.hasPeerPath(key.endpoint, key.route)})
	}
	routes := make(map[string]StreamingRouteStatus)
	for route, s := range t.closedRouteStats {
		routes[route] = s
	}
	for c := range t.conns {
		route := c.localIdentity.DialerGroup
		s := routes[route]
		s.Route = route
		s.UsefulBytes += c._stats.BytesReadUsefulData.Int64()
		s.DataBytes += c._stats.BytesReadData.Int64()
		if !c.closed.IsSet() {
			s.Sessions++
		}
		routes[route] = s
	}
	var out []StreamingRouteStatus
	for _, s := range routes {
		out = append(out, s)
	}
	return paths, out
}
