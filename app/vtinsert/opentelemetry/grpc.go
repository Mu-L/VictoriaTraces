package opentelemetry

import (
	"encoding/binary"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
	"io"
	"net/http"
)

// https://grpc.github.io/grpc/core/md_doc_statuscodes.html
const (
	GrpcOk                 = "0"
	GrpcCancelled          = "1"
	GrpcUnknown            = "2"
	GrpcInvalidArgument    = "3"
	GrpcDeadlineExceeded   = "4"
	GrpcNotFound           = "5"
	GrpcAlreadyExist       = "6"
	GrpcPermissionDenied   = "7"
	GrpcResourceExhausted  = "8"
	GrpcFailedPrecondition = "9"
	GrpcAbort              = "10"
	GrpcOutOfRange         = "11"
	GrpcUnimplemented      = "12"
	GrpcInternal           = "13"
	GrpcUnavailable        = "14"
	GrpcDataLoss           = "15"
	GrpcUnauthenticated    = "16"
)

// +------------+---------------------------------------------+
// |   1 byte   |                 4 bytes                     |
// +------------+---------------------------------------------+
// | Compressed |               Message Length                |
// |   Flag     |                 (uint32)                    |
// +------------+---------------------------------------------+
// |                                                          |
// |                   Message Data                           |
// |                 (variable length)                        |
// |                                                          |
// +----------------------------------------------------------+
// See https://grpc.github.io/grpc/core/md_doc__p_r_o_t_o_c_o_l-_h_t_t_p2.html
func getProtobufData(r *http.Request) ([]byte, error) {
	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, &httpserver.ErrorWithStatusCode{StatusCode: http.StatusInternalServerError, Err: fmt.Errorf("cannot read request body: %s", err)}
	}
	if len(reqBody) < 5 {
		return nil, &httpserver.ErrorWithStatusCode{StatusCode: http.StatusBadRequest, Err: fmt.Errorf("invalid grpc header length: %d", len(reqBody))}
	}
	grpcHeader := reqBody[:5]
	if isCompress := grpcHeader[0]; isCompress != 0 && isCompress != 1 {
		return nil, &httpserver.ErrorWithStatusCode{StatusCode: http.StatusBadRequest, Err: fmt.Errorf("grpc compression not supporte")}
	}
	messageLength := binary.BigEndian.Uint32(grpcHeader[1:5])
	if len(reqBody) != 5+int(messageLength) {
		return nil, &httpserver.ErrorWithStatusCode{StatusCode: http.StatusBadRequest, Err: fmt.Errorf("invalid message length: %d", messageLength)}
	}
	return reqBody[5:], nil
}

func writeErrorGrpcResponse(w http.ResponseWriter, grpcErrorCode, grpcErrorMessage string) {
	w.Header().Set("Content-Type", "application/grpc+proto")
	w.Header().Set("Trailer", "grpc-status, grpc-message")
	w.Header().Set("Grpc-Status", grpcErrorCode)
	w.Header().Set("Grpc-Message", grpcErrorMessage)
}

// https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#message-encoding
func writeExportTracesGrpcResponse(w http.ResponseWriter, rejectedSpans int64, errorMessage string) {
	resp := pb.ExportTraceServiceResponse{
		ExportTracePartialSuccess: pb.ExportTracePartialSuccess{
			RejectedSpans: rejectedSpans,
			ErrorMessage:  errorMessage,
		},
	}
	respData := resp.MarshalProtobuf(nil)

	grpcRespData := make([]byte, 5+len(respData))
	// Compressed flag
	grpcRespData[0] = 0
	// Message Length
	binary.BigEndian.PutUint32(grpcRespData[1:5], uint32(len(respData)))
	copy(grpcRespData[5:], respData)
	w.Header().Set("Content-Type", "application/grpc+proto")
	w.Header().Set("Trailer", "grpc-status, grpc-message")

	writtenLen, err := w.Write(grpcRespData)
	if writtenLen != len(grpcRespData) {
		logger.Errorf("unexpected write of %d bytes in replying OLTP export grpc request, expected:%d", writtenLen, len(grpcRespData))
		return
	}
	grpcStatus := GrpcOk

	if err != nil {
		grpcStatus = GrpcInternal
		grpcErrorMessage := fmt.Sprintf("failed to reply OLTP export grpc request , error:%s", err)
		logger.Errorf(grpcErrorMessage)
		w.Header().Set("Grpc-Message", grpcErrorMessage)
	}

	w.Header().Set("Grpc-Status", grpcStatus)
}
