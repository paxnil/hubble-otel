package hubblereceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestCreateReceiver(t *testing.T) {
	cfg := createDefaultConfig()

	settings := receivertest.NewNopCreateSettings()

	tracesReceiver, err := createTracesReceiver(context.Background(), settings, cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, tracesReceiver)

	logsReceiver, err := createLogsReceiver(context.Background(), settings, cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, logsReceiver)
}
