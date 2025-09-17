package opentelemetry

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"google.golang.org/grpc"

	"github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
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
	fmt.Println(req)
	return nil, nil
}

func MustStart(addr string, useProxyProtocol bool) *OTLPGrpcServer {
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
