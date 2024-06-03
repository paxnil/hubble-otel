package hubblereceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/cilium/hubble-otel/common"
	"github.com/cilium/hubble-otel/trace"

	"github.com/cilium/hubble-otel/hubblereceiver/internal/metadata"
)

var (
	typeStr = component.MustNewType("hubble")
)

var receivers = make(map[component.Config]*hubbleReceiver)

func getReceiver(cfg component.Config, settings receiver.CreateSettings) *hubbleReceiver {
	if r, ok := receivers[cfg]; ok {
		return r
	}
	r := newHubbleReceiver(cfg.(*Config), settings)
	receivers[cfg] = r
	return r
}
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		typeStr,
		createDefaultConfig,
		receiver.WithTraces(createTracesReceiver, metadata.TracesStability),
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	_true, _false := new(bool), new(bool)
	*_true, *_false = true, false

	defaultTraceEncoding, defaultLogEncoding := common.DefaultTraceEncoding, common.DefaultLogEncoding

	return &Config{
		BufferSize:                2048,
		FallbackServiceNamePrefix: common.OTelAttrServiceNameDefaultPrefix,
		TraceCacheWindow:          trace.DefaultTraceCacheWindow,
		ParseTraceHeaders:         true,
		FlowEncodingOptions: FlowEncodingOptions{
			Traces: common.EncodingOptions{
				Encoding:      &defaultTraceEncoding,
				LabelsAsMaps:  _true,
				HeadersAsMaps: _true,
				TopLevelKeys:  _true,
			},
			Logs: common.EncodingOptions{
				Encoding:         &defaultLogEncoding,
				LabelsAsMaps:     _true,
				HeadersAsMaps:    _true,
				TopLevelKeys:     _false,
				LogPayloadAsBody: _false,
			},
		},
		IncludeFlowTypes: IncludeFlowTypes{
			Traces: common.IncludeFlowTypes{},
			Logs:   common.IncludeFlowTypes{},
		},
	}
}

func createTracesReceiver(
	_ context.Context,
	settings receiver.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (receiver.Traces, error) {
	r := getReceiver(cfg, settings)
	if err := r.registerTraceConsumer(nextConsumer); err != nil {
		return nil, err
	}
	return r, nil
}

func createLogsReceiver(
	_ context.Context,
	settings receiver.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	r := getReceiver(cfg, settings)
	if err := r.registerLogsConsumer(nextConsumer); err != nil {
		return nil, err
	}
	return r, nil
}
