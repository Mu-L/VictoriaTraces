package pb

import (
	"errors"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
)

func init() {
	encoding.RegisterCodec(&easyProtoCodec{})
}

type easyProtoCodec struct{}

func (r *easyProtoCodec) Marshal(v any) ([]byte, error) {
	// unimplemented
	return nil, nil
}

func (r *easyProtoCodec) Unmarshal(data []byte, v any) error {
	// The OpenTelemetry trace service only expose 1 method and 1 argument, which is ExportTraceServiceRequest.
	// There's no other messages need to be unmarshalled here.
	// We can add support in the future if the protocol is changed.
	// See: https://github.com/open-telemetry/opentelemetry-proto/blob/v1.8.0/opentelemetry/proto/collector/trace/v1/trace_service.proto#L31
	req, ok := v.(*ExportTraceServiceRequest)
	if !ok {
		return errors.New("only ExportTraceServiceRequest supported")
	}
	return req.UnmarshalProtobuf(data)
}

func (r *easyProtoCodec) Name() string {
	// This codec name is duplicated to the default one and for overriding.
	// See: https://github.com/grpc/grpc-go/blob/v1.75.1/encoding/proto/proto.go#L33
	return proto.Name
}
