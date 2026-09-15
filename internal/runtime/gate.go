package runtime

import (
	"context"
	"sync"
)

type gate struct {
	once  sync.Once
	token chan struct{}
}

func (g *gate) acquire(ctx context.Context) error {
	g.once.Do(func() { g.token = make(chan struct{}, 1); g.token <- struct{}{} })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.token:
		if err := ctx.Err(); err != nil {
			g.release()
			return err
		}
		return nil
	}
}

func (g *gate) release() { g.token <- struct{}{} }
