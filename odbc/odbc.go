package odbc

import "C"
import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/jooita/sql/api"
)

const (
	BUFFER_SIZE     = 10 * 1024
	INFO_BUFFER_LEN = 256
)

var (
	Genv api.SQLHANDLE
)

type Connection struct {
	Dbc       api.SQLHANDLE
	connected bool
}

type Statement struct {
	executed   bool
	prepared   bool
	scrollable bool

	handle api.SQLHANDLE
}

func initEnv() (err error) {
	var out api.SQLHANDLE
	in := api.SQLHANDLE(api.SQL_NULL_HANDLE)
	ret := api.SQLAllocHandle(api.SQL_HANDLE_ENV, in, &out)
	if IsError(ret) {
		err := NewError("SQLAllocHandle", out)
		return err
	}
	Genv = out
	ret = api.SQLSetEnvAttr(api.SQLHENV(Genv), api.SQL_ATTR_ODBC_VERSION, api.SQLPOINTER(unsafe.Pointer(uintptr(api.SQL_OV_ODBC3))), api.SQLINTEGER(0))
	if IsError(ret) {
		err := NewError("SQLSetEnvAttr", api.SQLHENV(Genv))
		return err
	}
	return nil
}

func Connect(dsn string, params ...interface{}) (conn *Connection, err error) {
	var h api.SQLHANDLE
	ret := api.SQLAllocHandle(api.SQL_HANDLE_DBC, Genv, &h)
	if IsError(ret) {
		err := NewError("SQLAllocHandle", h)
		return nil, err
	}

	var stringLength2 api.SQLSMALLINT
	outBuf := make([]byte, BUFFER_SIZE*2)
	outConnectionString := (*api.SQLWCHAR)(unsafe.Pointer(&outBuf[0]))

	ret = api.SQLDriverConnect(api.SQLHDBC(h),
		api.SQLHWND(unsafe.Pointer(uintptr(0))),
		(*api.SQLWCHAR)(unsafe.Pointer(StringToUTF16Ptr(dsn))),
		api.SQL_NTS,
		outConnectionString,
		BUFFER_SIZE,
		&stringLength2,
		api.SQL_DRIVER_NOPROMPT)

	if IsError(ret) {
		err := NewError("SQLDriverConnect", api.SQLHDBC(h))
		return nil, err
	}
	return &Connection{Dbc: h, connected: true}, nil
}

func (conn *Connection) ExecDirect(sql string) (stmt *Statement, err error) {
	if stmt, err = conn.newStmt(); err != nil {
		return nil, err
	}
	wsql := StringToUTF16Ptr(sql)
	ret := api.SQLExecDirect(api.SQLHSTMT(stmt.handle), (*api.SQLWCHAR)(unsafe.Pointer(wsql)), api.SQL_NTS)
	if IsError(ret) {
		err := NewError("SQLExecDirect", api.SQLHSTMT(stmt.handle))
		stmt.Close()
		return nil, err
	}
	stmt.executed = true
	return stmt, nil
}

func (conn *Connection) newStmt() (*Statement, error) {
	stmt := &Statement{}

	ret := api.SQLAllocHandle(api.SQL_HANDLE_STMT, conn.Dbc, &stmt.handle)
	if IsError(ret) {
		err := NewError("SQLAllocHandle", conn.Dbc)
		return nil, err
	}
	return stmt, nil
}

func (conn *Connection) Prepare(sql string, params ...interface{}) (*Statement, error) {
	wsql := StringToUTF16Ptr(sql)
	stmt, err := conn.newStmt()
	if err != nil {
		return nil, err
	}
	ret := api.SQLPrepare(api.SQLHSTMT(stmt.handle), (*api.SQLWCHAR)(unsafe.Pointer(wsql)), api.SQLINTEGER(len(sql)))
	if IsError(ret) {
		err := NewError("SQLPrepare", api.SQLHSTMT(stmt.handle))
		stmt.Close()
		return nil, err
	}
	stmt.prepared = true
	return stmt, nil
}

func (conn *Connection) Commit() (err error) {
	ret := api.SQLEndTran(api.SQL_HANDLE_DBC, conn.Dbc, api.SQL_COMMIT)
	if IsError(ret) {
		err = NewError("SQLEndTran", conn.Dbc)
	}
	return
}

func (conn *Connection) AutoCommit(b bool) (err error) {
	var n C.int
	if b {
		n = api.SQL_AUTOCOMMIT_ON
	} else {
		n = api.SQL_AUTOCOMMIT_OFF
	}
	ret := api.SQLSetConnectAttr(api.SQLHDBC(conn.Dbc), api.SQL_ATTR_AUTOCOMMIT, api.SQLPOINTER(unsafe.Pointer(uintptr(n))), api.SQL_IS_UINTEGER)
	if IsError(ret) {
		err = NewError("SQLSetConnectAttr", api.SQLHDBC(conn.Dbc))
	}
	return
}

func (conn *Connection) BeginTransaction() (err error) {
	ret := api.SQLSetConnectAttr(api.SQLHDBC(conn.Dbc), api.SQL_ATTR_AUTOCOMMIT, api.SQLPOINTER(unsafe.Pointer(uintptr(api.SQL_AUTOCOMMIT_OFF))), api.SQL_IS_UINTEGER)
	if IsError(ret) {
		err = NewError("SQLSetConnectAttr", api.SQLHDBC(conn.Dbc))
	}
	return
}

func (conn *Connection) Rollback() (err error) {
	ret := api.SQLEndTran(api.SQL_HANDLE_DBC, conn.Dbc, api.SQL_ROLLBACK)
	if IsError(ret) {
		err = NewError("SQLEndTran", conn.Dbc)
	}
	return
}

func (conn *Connection) ServerInfo() (string, string, string, error) {
	var info_len api.SQLSMALLINT
	p := make([]byte, INFO_BUFFER_LEN)
	ret := api.SQLGetInfo(api.SQLHDBC(conn.Dbc), api.SQL_DATABASE_NAME, api.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if IsError(ret) {
		err := NewError("SQLGetInfo", api.SQLHDBC(conn.Dbc))
		return "", "", "", err
	}
	db := string(p[0:info_len])
	ret = api.SQLGetInfo(api.SQLHDBC(conn.Dbc), api.SQL_DBMS_VER, api.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if IsError(ret) {
		err := NewError("SQLGetInfo", api.SQLHDBC(conn.Dbc))
		return db, "", "", err
	}
	ver := string(p[0:info_len])
	ret = api.SQLGetInfo(api.SQLHDBC(conn.Dbc), api.SQL_SERVER_NAME, api.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if IsError(ret) {
		err := NewError("SQLGetInfo", api.SQLHDBC(conn.Dbc))
		return db, ver, "", err
	}
	server := string(p[0:info_len])
	return db, ver, server, nil
}

func (conn *Connection) ClientInfo() (string, string, string, error) {
	var info_len api.SQLSMALLINT
	p := make([]byte, INFO_BUFFER_LEN)
	ret := api.SQLGetInfo(api.SQLHDBC(conn.Dbc), api.SQL_DRIVER_NAME, api.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if IsError(ret) {
		err := NewError("SQLGetInfo", api.SQLHDBC(conn.Dbc))
		return "", "", "", err
	}
	drv_name := string(p[0:info_len])
	ret = api.SQLGetInfo(api.SQLHDBC(conn.Dbc), api.SQL_DRIVER_ODBC_VER, api.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if IsError(ret) {
		err := NewError("SQLGetInfo", api.SQLHDBC(conn.Dbc))
		return "", "", "", err
	}
	drv_odbc_ver := string(p[0:info_len])
	ret = api.SQLGetInfo(api.SQLHDBC(conn.Dbc), api.SQL_DRIVER_VER, api.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if IsError(ret) {
		err := NewError("SQLGetInfo", api.SQLHDBC(conn.Dbc))
		return "", "", "", err
	}
	drv_ver := string(p[0:info_len])
	return drv_name, drv_odbc_ver, drv_ver, nil
}

func (conn *Connection) Close() error {
	if conn.connected {
		ret := api.SQLDisconnect(api.SQLHDBC(conn.Dbc))
		if IsError(ret) {
			err := NewError("SQLDisconnect", api.SQLHDBC(conn.Dbc))
			return err
		}
		ret = api.SQLFreeHandle(api.SQL_HANDLE_DBC, conn.Dbc)
		if IsError(ret) {
			err := NewError("SQLFreeHandle", conn.Dbc)
			return err
		}
		conn.connected = false
	}
	return nil
}

func (stmt *Statement) RowsAffected() (int, error) {
	var nor api.SQLLEN
	ret := api.SQLRowCount(api.SQLHSTMT(stmt.handle), &nor)
	if IsError(ret) {
		err := NewError("SQLRowCount", api.SQLHSTMT(stmt.handle))
		return -1, err
	}
	return int(nor), nil
}

func (stmt *Statement) Cancel() error {
	ret := api.SQLCancel(api.SQLHSTMT(stmt.handle))
	if IsError(ret) {
		err := NewError("SQLCancel", api.SQLHSTMT(stmt.handle))
		return err
	}
	return nil
}

func (stmt *Statement) NumParams() int {
	var cParams api.SQLSMALLINT
	ret := api.SQLNumParams(api.SQLHSTMT(stmt.handle), &cParams)
	if IsError(ret) {
		return -1
	}
	return int(cParams)
}

func (stmt *Statement) Execute(params ...interface{}) error {
	if params != nil {
		var cParams api.SQLSMALLINT
		ret := api.SQLNumParams(api.SQLHSTMT(stmt.handle), &cParams)
		if IsError(ret) {
			err := NewError("SQLNumParams", api.SQLHSTMT(stmt.handle))
			return err
		}
		for i := 0; i < int(cParams); i++ {
			stmt.BindParam(i+1, params[i])
		}
	}
	ret := api.SQLExecute(api.SQLHSTMT(stmt.handle))
	if ret == api.SQL_NEED_DATA {
		// TODO
		//		send_data(stmt)
	} else if ret == api.SQL_NO_DATA {
		// Execute NO DATA
	} else if IsError(ret) {
		err := NewError("SQLExecute", api.SQLHSTMT(stmt.handle))
		return err
	}
	stmt.executed = true
	return nil
}

func (stmt *Statement) Execute2(params []driver.Value) error {
	if params != nil {
		var cParams api.SQLSMALLINT
		ret := api.SQLNumParams(api.SQLHSTMT(stmt.handle), &cParams)
		if IsError(ret) {
			err := NewError("SQLNumParams", api.SQLHSTMT(stmt.handle))
			return err
		}
		for i := 0; i < int(cParams); i++ {
			stmt.BindParam(i+1, params[i])
		}
	}
	ret := api.SQLExecute(api.SQLHSTMT(stmt.handle))
	if ret == api.SQL_NEED_DATA {
		// TODO
		//		send_data(stmt)
	} else if ret == api.SQL_NO_DATA {
		// Execute NO DATA
		// success but no data to report
	} else if IsError(ret) {
		err := NewError("SQLExecute", api.SQLHSTMT(stmt.handle))
		return err
	}
	stmt.executed = true
	return nil
}

func (stmt *Statement) Fetch() (bool, error) {
	ret := api.SQLFetch(api.SQLHSTMT(stmt.handle))
	if ret == api.SQL_NO_DATA {
		return false, nil
	}
	if IsError(ret) {
		err := NewError("SQLFetch", api.SQLHSTMT(stmt.handle))
		return false, err
	}
	return true, nil
}

type Row struct {
	Data []interface{}
}

// Get(Columnindex)
// TODO Get(ColumnName)
func (r *Row) Get(a interface{}) interface{} {
	value := reflect.ValueOf(a)
	switch f := value; f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return r.Data[f.Int()]
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return r.Data[f.Uint()]
		//	case *reflect.StringValue:
		//		i := r.Meta[f.Get()]
		//		return r.Data[i]
	}
	return nil
}

func (r *Row) GetInt(a interface{}) (ret int64) {
	v := r.Get(a)
	value := reflect.ValueOf(v)
	switch f := value; f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ret = int64(f.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		ret = int64(f.Uint())
	}
	return
}

func (r *Row) GetFloat(a interface{}) (ret float64) {
	v := r.Get(a)
	value := reflect.ValueOf(v)
	switch f := value; f.Kind() {
	case reflect.Float32, reflect.Float64:
		ret = float64(f.Float())
	}
	return
}

func (r *Row) GetString(a interface{}) (ret string) {
	v := r.Get(a)
	value := reflect.ValueOf(v)
	switch f := value; f.Kind() {
	case reflect.String:
		ret = f.String()
	}
	return
}

func (r *Row) Length() int {
	return len(r.Data)
}

func (stmt *Statement) FetchAll() (rows []*Row, err error) {
	for {
		row, err := stmt.FetchOne()
		if err != nil || row == nil {
			break
		}
		rows = append(rows, row)
	}

	return rows, err
}

func (stmt *Statement) FetchOne() (*Row, error) {
	ok, err := stmt.Fetch()
	if !ok {
		return nil, err
	}
	n, _ := stmt.NumFields()
	row := new(Row)
	row.Data = make([]interface{}, n)
	for i := 0; i < n; i++ {
		v, _, _, _ := stmt.GetField(i)
		row.Data[i] = v
	}
	return row, nil
}

func (stmt *Statement) FetchOne2(row []driver.Value) (eof bool, err error) {
	ok, err := stmt.Fetch()
	if !ok && err == nil {
		return !ok, nil
	} else if err != nil {
		return false, err
	}
	n, _ := stmt.NumFields()

	//if n != len(row) {return false, errors.New(fmt.Sprintf("argument length must be equal to %d", n))}

	for i := 0; i < n; i++ {
		v, _, _, _ := stmt.GetField(i)
		row[i] = v
	}
	return false, nil
}

func (stmt *Statement) GetField(field_index int) (v interface{}, ftype int, flen int, err error) {
	var field_type C.int
	var field_len api.SQLLEN
	var ll api.SQLSMALLINT

	ret := api.SQLColAttributeUIntPtr(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_DESC_CONCISE_TYPE, api.SQLPOINTER(unsafe.Pointer(uintptr(0))), api.SQLSMALLINT(0), &ll, unsafe.Pointer(&field_type))
	if IsError(ret) {
		return nil, 0, 0, err
	}
	ret = api.SQLColAttributeUIntPtr(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_DESC_LENGTH, api.SQLPOINTER(unsafe.Pointer(uintptr(0))), api.SQLSMALLINT(0), &ll, unsafe.Pointer(&field_len))
	if IsError(ret) {
		return nil, 0, 0, err
	}

	var fl api.SQLLEN = api.SQLLEN(field_len)
	switch int(field_type) {
	case api.SQL_BIT:
		var value api.BYTE
		ret = api.SQLGetData(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_C_BIT, api.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = byte(value)
		}
	case api.SQL_INTEGER, api.SQL_SMALLINT, api.SQL_TINYINT:
		var value C.long
		ret = api.SQLGetData(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_C_LONG, api.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = int(value)
		}
	case api.SQL_BIGINT:
		var value C.longlong
		ret = api.SQLGetData(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_C_SBIGINT, api.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = int64(value)
		}
	case api.SQL_REAL:
		var value C.float
		ret = api.SQLGetData(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_C_FLOAT, api.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = float32(value)
		}
	case api.SQL_FLOAT, api.SQL_DOUBLE, api.SQL_NUMERIC, api.SQL_DECIMAL:
		var value C.double
		ret = api.SQLGetData(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_C_DOUBLE, api.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = float64(value)
		}
	case api.SQL_WCHAR, api.SQL_WVARCHAR, api.SQL_WLONGVARCHAR:
		value := make([]uint16, (field_len+1)*2)
		ret = api.SQLGetData(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_C_WCHAR, api.SQLPOINTER(unsafe.Pointer(&value[0])), (field_len+1)*2, &fl)
		s := UTF16ToString(value)
		v = s

	case api.SQL_CHAR, api.SQL_VARCHAR, api.SQL_LONGVARCHAR:
		var result string
		var size int
		for {
			chunkSize := 1024
			value := make([]uint8, chunkSize)
			ret = api.SQLGetData(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_C_CHAR, api.SQLPOINTER(unsafe.Pointer(&value[0])), api.SQLLEN(chunkSize), &fl)
			if IsError(ret) || api.SQLLEN(ret) == api.SQL_NULL_DATA {
				v = nil
				break
			} else if ret == api.SQL_NO_DATA {
				break
			} else if ret == api.SQL_SUCCESS {
				result += string(value)
				size += int(fl)
				break
			}
			result += string(value)
			size += int(fl)
		}
		v = result[:size]
	case api.SQL_TYPE_TIMESTAMP, api.SQL_TYPE_DATE, api.SQL_TYPE_TIME, api.SQL_DATETIME:
		var value api.SQL_TIMESTAMP_STRUCT
		ret = api.SQLGetData(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_C_TYPE_TIMESTAMP, api.SQLPOINTER(unsafe.Pointer(&value)), api.SQLLEN(unsafe.Sizeof(value)), &fl)
		if fl == -1 {
			v = nil
		} else {
			v = time.Date(int(value.Year), time.Month(value.Month), int(value.Day), int(value.Hour), int(value.Minute), int(value.Second), int(value.Fraction), time.UTC)
		}
	case api.SQL_BINARY, api.SQL_VARBINARY, api.SQL_LONGVARBINARY:
		var vv int
		ret = api.SQLGetData(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_C_BINARY, api.SQLPOINTER(unsafe.Pointer(&vv)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			value := make([]byte, fl)
			ret = api.SQLGetData(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_C_BINARY, api.SQLPOINTER(unsafe.Pointer(&value[0])), api.SQLLEN(fl), &fl)
			v = value
		}
	default:
		value := make([]byte, field_len)
		ret = api.SQLGetData(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(field_index+1), api.SQL_C_BINARY, api.SQLPOINTER(unsafe.Pointer(&value[0])), field_len, &fl)
		v = value
	}
	if IsError(ret) {
		err = NewError("SQLGetData", api.SQLHSTMT(stmt.handle))
	}
	return v, int(field_type), int(fl), err
}

func (stmt *Statement) NumFields() (int, error) {
	var NOC api.SQLSMALLINT
	ret := api.SQLNumResultCols(api.SQLHSTMT(stmt.handle), &NOC)
	if IsError(ret) {
		err := NewError("SQLNumResultCols", api.SQLHSTMT(stmt.handle))
		return -1, err
	}
	return int(NOC), nil
}

func (stmt *Statement) GetParamType(index int) (int, int, int, int, error) {
	var data_type, dec_ptr, null_ptr api.SQLSMALLINT
	var size_ptr api.SQLULEN
	ret := api.SQLDescribeParam(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(index), &data_type, &size_ptr, &dec_ptr, &null_ptr)
	if IsError(ret) {
		err := NewError("SQLDescribeParam", api.SQLHSTMT(stmt.handle))
		return -1, -1, -1, -1, err
	}
	return int(data_type), int(size_ptr), int(dec_ptr), int(null_ptr), nil
}

func (stmt *Statement) BindParam(index int, param interface{}) error {
	var ValueType api.SQLSMALLINT
	var ParameterType api.SQLSMALLINT
	var ColumnSize api.SQLULEN
	var DecimalDigits api.SQLSMALLINT
	var ParameterValuePtr api.SQLPOINTER
	var BufferLength api.SQLLEN
	var StrLen_or_IndPt api.SQLLEN
	v := reflect.ValueOf(param)
	if param == nil {
		ft, _, _, _, err := stmt.GetParamType(index)
		if err != nil {
			return err
		}
		ParameterType = api.SQLSMALLINT(ft)
		if ParameterType == api.SQL_UNKNOWN_TYPE {
			ParameterType = api.SQL_VARCHAR
		}
		ValueType = api.SQL_C_DEFAULT
		StrLen_or_IndPt = api.SQL_NULL_DATA
		ColumnSize = 1
	} else {
		switch v.Kind() {
		case reflect.Bool:
			ParameterType = api.SQL_BIT
			ValueType = api.SQL_C_BIT
			var b [1]byte
			if v.Bool() {
				b[0] = 1
			} else {
				b[0] = 0
			}
			ParameterValuePtr = api.SQLPOINTER(unsafe.Pointer(&b[0]))
			BufferLength = 1
			StrLen_or_IndPt = 0
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			switch v.Type().Kind() {
			case reflect.Int:
			case reflect.Int8, reflect.Int16, reflect.Int32:
				ParameterType = api.SQL_INTEGER
				ValueType = api.SQL_C_LONG
				var l C.long = C.long(v.Int())
				ParameterValuePtr = api.SQLPOINTER(unsafe.Pointer(&l))
				BufferLength = 4
				StrLen_or_IndPt = 0
			case reflect.Int64:
				ParameterType = api.SQL_BIGINT
				ValueType = api.SQL_C_SBIGINT
				var ll C.longlong = C.longlong(v.Int())
				ParameterValuePtr = api.SQLPOINTER(unsafe.Pointer(&ll))
				BufferLength = 8
				StrLen_or_IndPt = 0
			}
		case reflect.Float32, reflect.Float64:
			ParameterType = api.SQL_DOUBLE
			ValueType = api.SQL_C_DOUBLE
			var d C.double = C.double(v.Float())
			ParameterValuePtr = api.SQLPOINTER(unsafe.Pointer(&d))
			BufferLength = 8
			StrLen_or_IndPt = 0
		case reflect.Complex64, reflect.Complex128:
		case reflect.String:
			var slen api.SQLUINTEGER = api.SQLUINTEGER(len(v.String()))
			ParameterType = api.SQL_VARCHAR
			ValueType = api.SQL_C_CHAR
			s := []byte(v.String())
			ParameterValuePtr = api.SQLPOINTER(unsafe.Pointer(&s[0]))
			ColumnSize = api.SQLULEN(slen)
			BufferLength = api.SQLLEN(slen + 1)
			StrLen_or_IndPt = api.SQLLEN(slen)
		default:
			fmt.Println("Not support type", v)
		}
	}
	ret := api.SQLBindParameter(api.SQLHSTMT(stmt.handle), api.SQLUSMALLINT(index), api.SQL_PARAM_INPUT, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, &StrLen_or_IndPt)
	if IsError(ret) {
		err := NewError("SQLBindParameter", api.SQLHSTMT(stmt.handle))
		return err
	}

	return nil
}

func (stmt *Statement) NextResult() bool {
	ret := api.SQLMoreResults(api.SQLHSTMT(stmt.handle))
	if ret == api.SQL_NO_DATA {
		return false
	}
	return true
}

func (stmt *Statement) NumRows() (int, error) {
	var NOR api.SQLLEN
	ret := api.SQLRowCount(api.SQLHSTMT(stmt.handle), &NOR)
	if IsError(ret) {
		err := NewError("SQLRowCount", api.SQLHSTMT(stmt.handle))
		return -1, err
	}
	return int(NOR), nil
}

func (stmt *Statement) HasRows() bool {
	n, _ := stmt.NumRows()
	return n > 0
}

type Field struct {
	Name          string
	Type          int
	Size          int
	DecimalDigits int
	Nullable      int
}

func (stmt *Statement) FieldMetadata(col int) (*Field, error) {
	var BufferLength api.SQLSMALLINT = INFO_BUFFER_LEN
	var NameLength api.SQLSMALLINT
	var DataType api.SQLSMALLINT
	var ColumnSize api.SQLULEN
	var DecimalDigits api.SQLSMALLINT
	var Nullable api.SQLSMALLINT
	ColumnName := make([]byte, INFO_BUFFER_LEN)
	ret := api.SQLDescribeCol(api.SQLHSTMT(stmt.handle),
		api.SQLUSMALLINT(col),
		(*api.SQLWCHAR)(unsafe.Pointer(&ColumnName[0])),
		BufferLength,
		&NameLength,
		&DataType,
		&ColumnSize,
		&DecimalDigits,
		&Nullable)
	if IsError(ret) {
		err := NewError("SQLDescribeCol", api.SQLHSTMT(stmt.handle))
		return nil, err
	}
	field := &Field{string(ColumnName[0:NameLength]), int(DataType), int(ColumnSize), int(DecimalDigits), int(Nullable)}
	return field, nil
}

func (stmt *Statement) free() {
	api.SQLFreeHandle(api.SQL_HANDLE_STMT, stmt.handle)
}

func (stmt *Statement) Close() {
	stmt.free()
}

func init() {
	if err := initEnv(); err != nil {
		panic("odbc init env error!" + err.Error())
	}
}
