package hubblereceiver

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/cilium/hubble-otel/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"
)

func TestLoadConfig(t *testing.T) {
	_ = os.Setenv("HUBBLE_ENDPOINT", "localhost:4244")
	_ = os.Setenv("NODE_NAME", "localhost")

	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	defaultConfig := factory.CreateDefaultConfig()
	defaultEncodingOptions := defaultConfig.(*Config).FlowEncodingOptions

	factories.Receivers[typeStr] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(path.Join(".", "testdata", "config.yaml"), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, len(cfg.Receivers), 4)

	r0 := cfg.Receivers[component.NewID(typeStr)]
	r0.(*Config).Endpoint = ""
	assert.Equal(t, r0, defaultConfig)
	assert.Equal(t, r0.(*Config), defaultConfig.(*Config))

	r1 := cfg.Receivers[component.NewIDWithName(typeStr, "customname")].(*Config)
	assert.Equal(t, r1.ReceiverSettings, []component.ID{component.NewIDWithName(typeStr, "customname")})

	r2 := cfg.Receivers[component.NewIDWithName(typeStr, "env")].(*Config)
	assert.Equal(t, r2.Endpoint, "localhost:4244")

	r3 := cfg.Receivers[component.NewIDWithName(typeStr, "nondefaultopts")].(*Config)
	assert.Equal(t, r3.Endpoint, "localhost:4244")
	assert.Equal(t, r3.FallbackServiceNamePrefix, common.OTelAttrServiceNameDefaultPrefix)
	assert.Equal(t, r3.TraceCacheWindow, time.Hour)
	assert.Equal(t, r3.ParseTraceHeaders, false)

	assert.Equal(t, *r3.FlowEncodingOptions.Traces.Encoding, "JSON")
	assert.Equal(t, *r3.FlowEncodingOptions.Traces.TopLevelKeys, !*defaultEncodingOptions.Traces.TopLevelKeys)
	assert.Equal(t, *r3.FlowEncodingOptions.Logs.LogPayloadAsBody, !*defaultEncodingOptions.Logs.LogPayloadAsBody)

	// this is to assert that currenly defaulting of unset fields is not based on CreateDefaultConfig(), which
	// is not ideal, but that is how collector configuration appears to work
	assert.Equal(t, *r3.FlowEncodingOptions.Traces.HeadersAsMaps, !*defaultEncodingOptions.Traces.HeadersAsMaps)
	assert.Equal(t, *r3.FlowEncodingOptions.Traces.LabelsAsMaps, !*defaultEncodingOptions.Traces.LabelsAsMaps)
	assert.Equal(t, *r3.FlowEncodingOptions.Logs.TopLevelKeys, !*defaultEncodingOptions.Logs.TopLevelKeys)
	assert.Equal(t, *r3.FlowEncodingOptions.Logs.HeadersAsMaps, !*defaultEncodingOptions.Logs.HeadersAsMaps)
	assert.Equal(t, *r3.FlowEncodingOptions.Logs.LabelsAsMaps, !*defaultEncodingOptions.Logs.LabelsAsMaps)

	assert.Equal(t, r3.IncludeFlowTypes.Traces, common.IncludeFlowTypes{"l7"})
	assert.Equal(t, r3.IncludeFlowTypes.Logs, common.IncludeFlowTypes{"all"})
}
