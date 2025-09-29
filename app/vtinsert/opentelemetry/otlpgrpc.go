package opentelemetry

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"

	"github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/insertutil"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
)

const OLTPExportTracesGrpcPath = "/opentelemetry.proto.collector.trace.v1.TraceService/Export"

// https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
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

func GrpcExportHandler(r *http.Request, w http.ResponseWriter) {
	if r.URL.Path != OLTPExportTracesGrpcPath {
		WriteErrorGrpcResponse(w, GrpcUnimplemented, fmt.Sprintf("grpc method not found: %s", r.URL.Path))
		return
	}
	cp, err := insertutil.GetCommonParams(r)
	if err != nil {
		WriteErrorGrpcResponse(w, GrpcInternal, fmt.Sprintf("cannot parse common params from request: %s", err))
		return
	}
	// stream fields must contain the service name and span name.
	// by using arguments and headers, users can also add other fields as stream fields
	// for potentially better efficiency.
	cp.StreamFields = append(mandatoryStreamFields, cp.StreamFields...)

	if err = insertutil.CanWriteData(); err != nil {
		WriteErrorGrpcResponse(w, GrpcInternal, err.Error())
		return
	}

	protobufData, err := getProtobufData(r)
	if err != nil {
		WriteErrorGrpcResponse(w, GrpcInternal, fmt.Sprintf("failed to get protobuf data from request, error: %s", err))
		return
	}
	encoding := r.Header.Get("grpc-encoding")

	err = protoparserutil.ReadUncompressedData(bytes.NewReader(protobufData), encoding, maxRequestSize, func(data []byte) error {
		var (
			req         otelpb.ExportTraceServiceRequest
			callbackErr error
		)
		lmp := cp.NewLogMessageProcessor("opentelemetry_traces", false)
		if callbackErr = req.UnmarshalProtobuf(data); callbackErr != nil {
			return fmt.Errorf("cannot unmarshal request from %d protobuf bytes: %w", len(data), callbackErr)
		}
		callbackErr = pushExportTraceServiceRequest(&req, lmp)
		lmp.MustClose()
		return callbackErr
	})

	if err != nil {
		WriteErrorGrpcResponse(w, GrpcInternal, fmt.Sprintf("cannot read OpenTelemetry protocol data: %s", err))
		return
	}

	writeExportTracesGrpcResponse(w, 0, "")
	return
}

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

func WriteErrorGrpcResponse(w http.ResponseWriter, grpcErrorCode, grpcErrorMessage string) {
	w.Header().Set("Content-Type", "application/grpc+proto")
	w.Header().Set("Trailer", "grpc-status, grpc-message")
	w.Header().Set("Grpc-Status", grpcErrorCode)
	w.Header().Set("Grpc-Message", grpcErrorMessage)
}

// https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#message-encoding
func writeExportTracesGrpcResponse(w http.ResponseWriter, rejectedSpans int64, errorMessage string) {
	var respData []byte
	// The server MUST leave the partial_success field unset in case of a successful response.
	// https://opentelemetry.io/docs/specs/otlp/#full-success
	if rejectedSpans != 0 || errorMessage == "" {
		resp := otelpb.ExportTraceServiceResponse{
			ExportTracePartialSuccess: otelpb.ExportTracePartialSuccess{
				RejectedSpans: rejectedSpans,
				ErrorMessage:  errorMessage,
			},
		}
		respData = resp.MarshalProtobuf(nil)
	}

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
