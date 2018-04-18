package odbc

import (
	"github.com/jooita/sql/api"
	"database/sql/driver"
	"fmt"
	"strings"
	"unsafe"
)

func IsError(ret api.SQLRETURN) bool {
	return !(ret == api.SQL_SUCCESS || ret == api.SQL_SUCCESS_WITH_INFO)
}

type DiagRecord struct {
	State       string
	NativeError int
	Message     string
}

func (r *DiagRecord) String() string {
	return fmt.Sprintf("{%s} %s", r.State, r.Message)
}

type Error struct {
	APIName string
	Diag    []DiagRecord
}

func (e *Error) Error() string {
	ss := make([]string, len(e.Diag))
	for i, r := range e.Diag {
		ss[i] = r.String()
	}
	return e.APIName + ": " + strings.Join(ss, "\n")
}

func NewError(apiName string, handle interface{}) error {
	h, ht, herr := ToHandleAndType(handle)
	if herr != nil {
		return herr
	}
	err := &Error{APIName: apiName}
	var ne api.SQLINTEGER
	state := make([]uint16, 6)
	msg := make([]uint16, api.SQL_MAX_MESSAGE_LENGTH)
	for i := 1; ; i++ {
		ret := api.SQLGetDiagRec(ht, h, api.SQLSMALLINT(i),
			(*api.SQLWCHAR)(unsafe.Pointer(&state[0])), &ne,
			(*api.SQLWCHAR)(unsafe.Pointer(&msg[0])),
			api.SQLSMALLINT(len(msg)), nil)
		if ret == api.SQL_NO_DATA {
			break
		}
		if IsError(ret) {
			return fmt.Errorf("SQLGetDiagRec failed: ret=%d", ret)
		}
		r := DiagRecord{
			State:       UTF16ToString(state),
			NativeError: int(ne),
			Message:     UTF16ToString(msg),
		}
		if r.State == "08S01" {
			return driver.ErrBadConn
		}
		err.Diag = append(err.Diag, r)
	}
	return err
}
