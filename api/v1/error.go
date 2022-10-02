package log_v1

import (
	"fmt"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrorOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrorOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(
		404,
		fmt.Sprintf("Offset out of range :%d", e.Offset))
	message := fmt.Sprintf("The requeseted offset is outside the log's range: %d", e.Offset)

	details := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: message,
	}
	statusWithDetails, err := st.WithDetails(details)
	if err != nil {
		return st
	}
	return statusWithDetails
}

func (e ErrorOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
