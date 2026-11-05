package torrent

import (
	"context"
	"errors"
	"fmt"
)

type dialPool struct {
	resCh  chan DialResult
	addr   string
	left   int
	errors []error
}

func (me *dialPool) getFirst() (res DialResult) {
	for me.left > 0 && res.Conn == nil {
		res = <-me.resCh
		me.left--
		if res.Err != nil {
			me.errors = append(me.errors, fmt.Errorf("%s: %w", res.Dialer.DialerNetwork(), res.Err))
		}
	}
	if res.Conn == nil {
		res.Err = errors.Join(me.errors...)
	}
	return
}

func (me *dialPool) add(ctx context.Context, dialer Dialer) {
	me.left++
	go func() {
		conn, err := dialFromSocket(ctx, dialer, me.addr)
		me.resCh <- DialResult{Conn: conn, Dialer: dialer, Err: err}
	}()
}

func (me *dialPool) startDrainer() {
	go me.drainAndCloseRemainingDials()
}

func (me *dialPool) drainAndCloseRemainingDials() {
	for me.left > 0 {
		conn := (<-me.resCh).Conn
		me.left--
		if conn != nil {
			conn.Close()
		}
	}
}
