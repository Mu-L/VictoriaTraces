package opentelemetry

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"github.com/VictoriaMetrics/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
)

var (
	requestsGRPCTotal = metrics.NewCounter(fmt.Sprintf(`vt_grpc_requests_total{path="%s"}`, pb.TraceService_Export_FullMethodName))
	errorsGRPCTotal   = metrics.NewCounter(fmt.Sprintf(`vt_grpc_errors_total{path="%s"}`, pb.TraceService_Export_FullMethodName))

	requestGRPCDuration = metrics.NewSummary(fmt.Sprintf(`vt_grpc_request_duration_seconds{path="%s"}`, pb.TraceService_Export_FullMethodName))
)

type OTLPGrpcServer struct {
	pb.UnimplementedTraceServiceServer

	addr       string
	lnTCP      net.Listener
	grpcServer *grpc.Server
	wg         sync.WaitGroup
	cm         ConnsMap
}

func (g *OTLPGrpcServer) Export(ctx context.Context, req *pb.ExportTraceServiceRequest) (*pb.ExportTraceServiceResponse, error) {
	startTime := time.Now()
	requestsGRPCTotal.Inc()

	md, _ := metadata.FromIncomingContext(ctx)
	cp, err := insertutil.GetMetadataCommonParams(md)
	if err != nil {
		return nil, err
	}
	// stream fields must contain the service name and span name.
	// by using arguments and headers, users can also add other fields as stream fields
	// for potentially better efficiency.
	cp.StreamFields = append(mandatoryStreamFields, cp.StreamFields...)

	if err = insertutil.CanWriteData(); err != nil {
		return nil, err
	}

	lmp := cp.NewLogMessageProcessor("opentelemetry_traces", false)
	err = pushExportTraceServiceRequest(req, lmp)
	if err != nil {
		errorsGRPCTotal.Inc()
		return nil, err
	}

	// update requestGRPCDuration only for successfully parsed requests
	// There is no need in updating requestGRPCDuration for request errors,
	// since their timings are usually much smaller than the timing for successful request parsing.
	requestGRPCDuration.UpdateDuration(startTime)
	return &pb.ExportTraceServiceResponse{}, nil
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
		pb.RegisterTraceServiceServer(s.grpcServer, s)
		s.grpcServer.Serve(s.lnTCP)
		logger.Infof("stopped TCP InfluxDB OTLPGrpcServer at %q", addr)
	}()
	return s
}

// MustStop stops the OTLPGrpcServer.
func (s *OTLPGrpcServer) MustStop() {
	logger.Infof("stopping OTLP gRPC OTLPGrpcServer at %q...", s.addr)
	s.grpcServer.GracefulStop()
	if err := s.lnTCP.Close(); err != nil {
		logger.Errorf("cannot close OTLP gRPC OTLPGrpcServer: %s", err)
	}
	s.cm.CloseAll(0)
	s.wg.Wait()
	logger.Infof("OTLP gRPC OTLPGrpcServer at %q have been stopped", s.addr)
}
