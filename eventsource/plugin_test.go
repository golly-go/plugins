package eventsource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPlugin_ProjectionWorkerAndBufferOptions(t *testing.T) {
	tests := []struct {
		name               string
		opts               []PluginOption
		defaultWorkers     int
		defaultBufferSize  int
		expectedNumWorkers int
		expectedPerWorker  int
	}{
		{
			name:               "no options falls back to package defaults",
			opts:               nil,
			defaultWorkers:     6,
			defaultBufferSize:  600,
			expectedNumWorkers: 6,
			expectedPerWorker:  minProjectionWorkerBuffer, // 600/6=100, floored to 128
		},
		{
			name:               "PluginWithProjectionWorkers overrides worker count only",
			opts:               []PluginOption{PluginWithProjectionWorkers(3)},
			defaultWorkers:     6,
			defaultBufferSize:  600,
			expectedNumWorkers: 3,
			expectedPerWorker:  200,
		},
		{
			name:               "PluginWithProjectionBufferSize overrides buffer only",
			opts:               []PluginOption{PluginWithProjectionBufferSize(1200)},
			defaultWorkers:     6,
			defaultBufferSize:  600,
			expectedNumWorkers: 6,
			expectedPerWorker:  200,
		},
		{
			name: "both options override the package defaults",
			opts: []PluginOption{
				PluginWithProjectionWorkers(3),
				PluginWithProjectionBufferSize(1200),
			},
			defaultWorkers:     6,
			defaultBufferSize:  600,
			expectedNumWorkers: 3,
			expectedPerWorker:  400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withDefaultProjectionWorkers(t, tt.defaultWorkers)
			withDefaultProjectionBufferSize(t, tt.defaultBufferSize)

			opts := append([]PluginOption{PluginWithStore(NewInMemoryStore())}, tt.opts...)
			p := NewPlugin(opts...)

			require.NotNil(t, p.Engine())
			pm := p.Engine().Projections()
			require.NotNil(t, pm)
			assert.Equal(t, tt.expectedNumWorkers, pm.numWorkers)
			require.Len(t, pm.workers, tt.expectedNumWorkers)
			for _, ch := range pm.workers {
				assert.Equal(t, tt.expectedPerWorker, cap(ch))
			}
		})
	}
}

func TestNewPlugin_ProjectionWorkerOptions_NoOpWhenEngineSupplied(t *testing.T) {
	withDefaultProjectionWorkers(t, 6)
	withDefaultProjectionBufferSize(t, 600)

	preconfigured := NewEngine(WithStore(NewInMemoryStore()), WithProjectionWorkers(9))

	p := NewPlugin(
		PluginWithEngine(preconfigured),
		PluginWithProjectionWorkers(3),
		PluginWithProjectionBufferSize(1200),
	)

	// The plugin-level projection options only apply when building a default
	// engine; a directly supplied engine's own configuration wins.
	assert.Same(t, preconfigured, p.Engine())
	assert.Equal(t, 9, p.Engine().Projections().numWorkers)
}
