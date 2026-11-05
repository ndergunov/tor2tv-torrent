# tor2tv peer identity extension

Based on upstream anacrolix/torrent **v1.61.0**, commit
`486bcbe758e0cff0f750fcee9c26268a11250d77`. Original module path and license
are preserved. This fork is pinned by commit through a Go module replacement
in [tor2tv](https://github.com/ndergunov/tor2tv), issue #211.

The optional `PeerIdentityForConn` callback resolves the local peer ID, listen
port and dialer group once per accepted or initiated connection, before MSE
and BitTorrent handshakes. Each identity advertises its own BEP 10 port.
`AdditionalPeerIDs` prevents connections to any of the client's own identities.
Duplicate remote peer IDs are checked separately for each local identity.

`PeerDialerGroups` starts one independent attempt per requested group.
Dialers tag themselves with `PeerDialerGroup() string`; the TCP/uTP race
remains within each group. Pending and established connections are checked
per endpoint and group. All identities share the original scheduler, cache,
connection limits and torrent-level tracker accounting. Tracker, DHT and
WebTorrent announcements retain the primary identity.

The callbacks and identity lists must be immutable after Client construction.
With nil callbacks, connection selection retains upstream behavior.

Validation includes the upstream root package short tests and tor2tv's
`TestDualIdentitiesOnePeer`: a single seed endpoint supplies two distinct,
hash-verified pieces to two simultaneous identities over TCP and uTP, with
plaintext and RC4 MSE, plus incoming-connection coverage. The seed checks the
peer IDs and advertised ports while tor2tv verifies the single shared cache.

The fork also fixes a lost wake in the message writer: subscribe to the update
signal before checking request state. Otherwise an unchoke or reader update
between the state check and waiting can stall the first request indefinitely.
`TestMessageWriterRetainsWakeDuringFill` reproduces the failure deterministically
on the upstream implementation and passes after the fix.

Optional streaming scheduling (`RequestUpdateInterval` and
`StreamUrgentRequestTimeout`, both disabled by default) periodically revisits
pending requests even when an idle alternate session has no requests of its
own. Reader-head blocks are ordered by byte offset. A block held past its
playback deadline can be reassigned despite ordinary request-count balancing;
traffic for unrelated blocks does not extend that deadline. Normal background
piece selection remains unchanged. tor2tv uses 1 second updates and a 2 second
reader-head deadline in dual-identity mode. Its
`TestDualIdentityRecoversStalledReaderHead` demonstrates a reader that stalls
with the original policy and recovers with the streaming policy while both
sessions remain connected.

`StreamUrgentWindow` assigns `PiecePriorityNext` to a configurable near-future
reader region, below the current `PiecePriorityNow` and above ordinary
readahead. The region follows seeks and is bounded by each reader's window.
The current block keeps its 2 second reassignment deadline; near-future blocks
are sorted sequentially without that immediate-playback deadline. tor2tv uses
64 MiB of high-priority near-future data (about 5.4 seconds at 100 Mbit/s)
within its existing 256 MiB readahead window. Its
`TestStreamingUrgentWindowFollowsSeek` checks priority boundaries and seeks.
