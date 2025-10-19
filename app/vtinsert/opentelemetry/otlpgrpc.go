package opentelemetry

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"github.com/VictoriaMetrics/metrics"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"

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
