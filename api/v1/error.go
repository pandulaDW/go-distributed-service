package v1

import (
	"fmt"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
	"net/http"
)

type ErrOffSetOutOfRange struct {
	Offset uint64
}

func (e ErrOffSetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(http.StatusNotFound, fmt.Sprintf("offset out of range: %d", e))
	msg := fmt.Sprintf("The requested offset is outside the log's range: %d", e.Offset)
	d := &errdetails.LocalizedMessage{Locale: "en-US", Message: msg}
	std, err := st.WithDetails(d)
	if err != nil {
		return nil
	}
	return std
}

func (e ErrOffSetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
