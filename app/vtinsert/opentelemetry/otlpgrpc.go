package opentelemetry

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"github.com/VictoriaMetrics/metrics"
	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/insertutil"
	collector "github.com/VictoriaMetrics/VictoriaTraces/lib/proto/opentelemetry/proto/collector/trace/v1"
	v1 "github.com/VictoriaMetrics/VictoriaTraces/lib/proto/opentelemetry/proto/common/v1"
	v2 "github.com/VictoriaMetrics/VictoriaTraces/lib/proto/opentelemetry/proto/trace/v1"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
)

var (
	requestsGRPCTotal = metrics.NewCounter(fmt.Sprintf(`vt_grpc_requests_total{path="%s"}`, collector.TraceService_Export_FullMethodName))
	errorsGRPCTotal   = metrics.NewCounter(fmt.Sprintf(`vt_grpc_errors_total{path="%s"}`, collector.TraceService_Export_FullMethodName))

	requestGRPCDuration = metrics.NewSummary(fmt.Sprintf(`vt_grpc_request_duration_seconds{path="%s"}`, collector.TraceService_Export_FullMethodName))
)

type OTLPGrpcServer struct {
	collector.UnimplementedTraceServiceServer

	addr       string
	lnTCP      net.Listener
	grpcServer *grpc.Server
	wg         sync.WaitGroup
	cm         ConnsMap
}

func (g *OTLPGrpcServer) Export(ctx context.Context, req *collector.ExportTraceServiceRequest) (*collector.ExportTraceServiceResponse, error) {
	return &collector.ExportTraceServiceResponse{}, nil
}

func MustStartOTLPServer(addr string, useProxyProtocol bool) *OTLPGrpcServer {
	logger.Infof("starting OTLP gRPC OTLPGrpcServer at %q", addr)
	lnTCP, err := netutil.NewTCPListener("otlpgrpc", addr, useProxyProtocol, nil)
	if err != nil {
		logger.Fatalf("cannot start OTLP gRPC OTLPGrpcServer at %q: %s", addr, err)
	}

	s := &OTLPGrpcServer{
		addr:  addr,
		lnTCP: lnTCP,
	}
	s.cm.Init("otlpgrpc")
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		var opts []grpc.ServerOption
		s.grpcServer = grpc.NewServer(opts...)
		collector.RegisterTraceServiceServer(s.grpcServer, s)
		s.grpcServer.Serve(s.lnTCP)
	}()
	return s
}

// MustStop stops the OTLPGrpcServer.
func (s *OTLPGrpcServer) MustStop() {
	logger.Infof("stopping OTLP gRPC OTLPGrpcServer at %q...", s.addr)
	s.grpcServer.GracefulStop()
	s.cm.CloseAll(0)
	s.wg.Wait()
	logger.Infof("OTLP gRPC OTLPGrpcServer at %q have been stopped", s.addr)
}

// pushGRPCExportTraceServiceRequest is the entry point of OTLP data processing. It should be called by different
// request handlers such as OTLPgRPC handler.
func pushGRPCExportTraceServiceRequest(req *collector.ExportTraceServiceRequest, lmp insertutil.LogMessageProcessor) error {
	var commonFields []logstorage.Field
	for _, rs := range req.ResourceSpans {
		commonFields = commonFields[:0]
		attributes := rs.Resource.Attributes
		commonFields = appendGRPCKeyValuesWithPrefix(commonFields, attributes, "", otelpb.ResourceAttrPrefix)
		commonFieldsLen := len(commonFields)
		for _, ss := range rs.ScopeSpans {
			commonFields = pushGRPCFieldsFromScopeSpans(ss, commonFields[:commonFieldsLen], lmp)
		}
	}
	return nil
}

func pushGRPCFieldsFromScopeSpans(ss *v2.ScopeSpans, commonFields []logstorage.Field, lmp insertutil.LogMessageProcessor) []logstorage.Field {
	commonFields = append(commonFields, logstorage.Field{
		Name:  otelpb.InstrumentationScopeName,
		Value: ss.Scope.GetName(),
	}, logstorage.Field{
		Name:  otelpb.InstrumentationScopeVersion,
		Value: ss.Scope.GetVersion(),
	})
	commonFields = appendGRPCKeyValuesWithPrefix(commonFields, ss.Scope.Attributes, "", otelpb.InstrumentationScopeAttrPrefix)
	commonFieldsLen := len(commonFields)
	for _, span := range ss.Spans {
		commonFields = pushGRPCFieldsFromSpan(span, commonFields[:commonFieldsLen], lmp)
	}
	return commonFields
}

func pushGRPCFieldsFromSpan(span *v2.Span, scopeCommonFields []logstorage.Field, lmp insertutil.LogMessageProcessor) []logstorage.Field {
	fields := scopeCommonFields
	fields = append(fields,
		logstorage.Field{Name: otelpb.SpanIDField, Value: hex.EncodeToString(span.SpanId)},
		logstorage.Field{Name: otelpb.TraceStateField, Value: span.TraceState},
		logstorage.Field{Name: otelpb.ParentSpanIDField, Value: hex.EncodeToString(span.ParentSpanId)},
		logstorage.Field{Name: otelpb.FlagsField, Value: strconv.FormatUint(uint64(span.Flags), 10)},
		logstorage.Field{Name: otelpb.NameField, Value: span.Name},
		logstorage.Field{Name: otelpb.KindField, Value: strconv.FormatInt(int64(span.Kind), 10)},
		logstorage.Field{Name: otelpb.StartTimeUnixNanoField, Value: strconv.FormatUint(span.StartTimeUnixNano, 10)},
		logstorage.Field{Name: otelpb.EndTimeUnixNanoField, Value: strconv.FormatUint(span.EndTimeUnixNano, 10)},
		logstorage.Field{Name: otelpb.DurationField, Value: strconv.FormatUint(span.EndTimeUnixNano-span.StartTimeUnixNano, 10)},

		logstorage.Field{Name: otelpb.DroppedAttributesCountField, Value: strconv.FormatUint(uint64(span.DroppedAttributesCount), 10)},
		logstorage.Field{Name: otelpb.DroppedEventsCountField, Value: strconv.FormatUint(uint64(span.DroppedEventsCount), 10)},
		logstorage.Field{Name: otelpb.DroppedLinksCountField, Value: strconv.FormatUint(uint64(span.DroppedLinksCount), 10)},

		logstorage.Field{Name: otelpb.StatusMessageField, Value: span.Status.Message},
		logstorage.Field{Name: otelpb.StatusCodeField, Value: strconv.FormatInt(int64(span.Status.Code), 10)},
	)

	// append span attributes
	fields = appendGRPCKeyValuesWithPrefix(fields, span.Attributes, "", otelpb.SpanAttrPrefixField)

	for idx, event := range span.Events {
		eventFieldPrefix := otelpb.EventPrefix
		eventFieldSuffix := ":" + strconv.Itoa(idx)
		fields = append(fields,
			logstorage.Field{Name: eventFieldPrefix + otelpb.EventTimeUnixNanoField + eventFieldSuffix, Value: strconv.FormatUint(event.TimeUnixNano, 10)},
			logstorage.Field{Name: eventFieldPrefix + otelpb.EventNameField + eventFieldSuffix, Value: event.Name},
			logstorage.Field{Name: eventFieldPrefix + otelpb.EventDroppedAttributesCountField + eventFieldSuffix, Value: strconv.FormatUint(uint64(event.DroppedAttributesCount), 10)},
		)
		// append event attributes
		fields = appendGRPCKeyValuesWithPrefixSuffix(fields, event.Attributes, "", eventFieldPrefix+otelpb.EventAttrPrefix, eventFieldSuffix)
	}

	for idx, link := range span.Links {
		linkFieldPrefix := otelpb.LinkPrefix
		linkFieldSuffix := ":" + strconv.Itoa(idx)
		fields = append(fields,
			logstorage.Field{Name: linkFieldPrefix + otelpb.LinkTraceIDField + linkFieldSuffix, Value: hex.EncodeToString(link.TraceId)},
			logstorage.Field{Name: linkFieldPrefix + otelpb.LinkSpanIDField + linkFieldSuffix, Value: hex.EncodeToString(link.SpanId)},
			logstorage.Field{Name: linkFieldPrefix + otelpb.LinkTraceStateField + linkFieldSuffix, Value: link.TraceState},
			logstorage.Field{Name: linkFieldPrefix + otelpb.LinkDroppedAttributesCountField + linkFieldSuffix, Value: strconv.FormatUint(uint64(link.DroppedAttributesCount), 10)},
			logstorage.Field{Name: linkFieldPrefix + otelpb.LinkFlagsField + linkFieldSuffix, Value: strconv.FormatUint(uint64(link.Flags), 10)},
		)

		// append link attributes
		fields = appendGRPCKeyValuesWithPrefixSuffix(fields, link.Attributes, "", linkFieldPrefix+otelpb.LinkAttrPrefix, linkFieldSuffix)
	}
	fields = append(fields,
		logstorage.Field{Name: "_msg", Value: msgFieldValue},
		// MUST: always place TraceIDField at the last. The Trace ID is required for data distribution.
		// Placing it at the last position helps netinsert to find it easily, without adding extra field to
		// *logstorage.InsertRow structure, which is required due to the sync between logstorage and VictoriaTraces.
		// todo: @jiekun the trace ID field MUST be the last field. add extra ways to secure it.
		logstorage.Field{Name: otelpb.TraceIDField, Value: hex.EncodeToString(span.TraceId)},
	)

	// Create an entry in the trace-id-idx stream if this trace_id hasn't been seen before.
	// The index entry must be written first to ensure that an index always exists for the data.
	// During querying, if no index is found, the data must not exist.
	if !traceIDCache.Has(span.TraceId) {
		lmp.AddRow(int64(span.StartTimeUnixNano), []logstorage.Field{
			{Name: "_msg", Value: msgFieldValue},
			// todo: @jiekun the trace ID field MUST be the last field. add extra ways to secure it.
			{Name: otelpb.TraceIDIndexFieldName, Value: hex.EncodeToString(span.TraceId)},
		}, []logstorage.Field{{Name: otelpb.TraceIDIndexStreamName, Value: strconv.FormatUint(xxhash.Sum64(span.TraceId)%otelpb.TraceIDIndexPartitionCount, 10)}})
		traceIDCache.Set(span.TraceId, nil)
	}

	lmp.AddRow(int64(span.EndTimeUnixNano), fields, nil)

	return fields
}

func appendGRPCKeyValuesWithPrefix(fields []logstorage.Field, kvs []*v1.KeyValue, parentField, prefix string) []logstorage.Field {
	return appendGRPCKeyValuesWithPrefixSuffix(fields, kvs, parentField, prefix, "")
}

func appendGRPCKeyValuesWithPrefixSuffix(fields []logstorage.Field, kvs []*v1.KeyValue, parentField, prefix, suffix string) []logstorage.Field {
	for _, attr := range kvs {
		fieldName := attr.GetKey()
		if parentField != "" {
			fieldName = parentField + "." + fieldName
		}

		v := attr.GetValue()
		if v == nil {
			fields = append(fields, logstorage.Field{
				Name:  prefix + fieldName + suffix,
				Value: "-",
			})
			continue
		}

		switch x := v.Value.(type) {
		case *v1.AnyValue_StringValue:
			fields = append(fields, logstorage.Field{
				Name:  prefix + fieldName + suffix,
				Value: x.StringValue,
			})
		case *v1.AnyValue_BoolValue:
			fields = append(fields, logstorage.Field{
				Name:  prefix + fieldName + suffix,
				Value: strconv.FormatBool(x.BoolValue),
			})
		case *v1.AnyValue_BytesValue:
			fields = append(fields, logstorage.Field{
				Name:  prefix + fieldName + suffix,
				Value: string(x.BytesValue),
			})
		case *v1.AnyValue_IntValue:
			fields = append(fields, logstorage.Field{
				Name:  prefix + fieldName + suffix,
				Value: strconv.FormatInt(x.IntValue, 10),
			})
		case *v1.AnyValue_DoubleValue:
			fields = append(fields, logstorage.Field{
				Name:  prefix + fieldName + suffix,
				Value: strconv.FormatFloat(x.DoubleValue, 'g', -1, 64),
			})
		case *v1.AnyValue_KvlistValue:
			fields = appendGRPCKeyValuesWithPrefixSuffix(fields, x.KvlistValue.Values, fieldName, prefix, suffix)
		default:
			fields = append(fields, logstorage.Field{
				Name:  prefix + fieldName + suffix,
				Value: v.String(),
			})
		}
	}
	return fields
}
