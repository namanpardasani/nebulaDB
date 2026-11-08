package engine_test

import (
	"testing"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/engine/enginetest"
)

func TestMemConformance(t *testing.T) {
	enginetest.Run(t, func(t *testing.T, now func() int64) engine.Engine {
		return engine.NewMemClock(now)
	})
}
