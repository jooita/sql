package dataframe

/*
#include <stdio.h>
#include <string.h>
*/
import "C"
import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/jooita/sql/api"
	"github.com/jooita/sql/odbc"
)

func (df *DataFrame) getColumns(conn *odbc.Connection, table string) error {
	stmt, err := conn.Prepare(fmt.Sprintf("select * from %s", table))
	if err != nil {
		return err
	}
	err = stmt.Execute()
	if err != nil {
		return err
	}
	rows, err := stmt.FetchAll()
	if err != nil {
		return err
	}
	for _, row := range rows {
		df.AddRow(row.Data)
	}
	stmt.Close()
	conn.Close()

	return nil
}

func (df *DataFrame) getColumnInfo(conn *odbc.Connection, table string) ([]ColumnInfo, error) {

	//Auto Get ColumnInfo...
	//get statement.
	var out api.SQLHANDLE
	ret := api.SQLAllocHandle(api.SQL_HANDLE_STMT, conn.Dbc, &out)
	if odbc.IsError(ret) {
		return nil, odbc.NewError("SQLAllocHandle", conn.Dbc)
	}
	hstmt := api.SQLHSTMT(out)

	ret = api.SQLSetStmtUIntPtrAttr(hstmt, api.SQL_ATTR_CURSOR_SCROLLABLE, api.SQL_SCROLLABLE, 0)
	if odbc.IsError(ret) {
		return nil, odbc.NewError("SQLSetStmtAttr", hstmt)
	}

	ret = api.SQLSetStmtUIntPtrAttr(hstmt, api.SQL_ATTR_CURSOR_SENSITIVITY, api.SQL_INSENSITIVE, 0)
	if odbc.IsError(ret) {
		return nil, odbc.NewError("SQLSetStmtAttr", hstmt)
	}

	//get table.
	query := fmt.Sprintf("SELECT * FROM %s", table)
	b := odbc.StringToUTF16(query)
	ret = api.SQLExecDirect(hstmt, (*api.SQLWCHAR)(unsafe.Pointer(&b[0])), api.SQLINTEGER(len(query)))
	if odbc.IsError(ret) {
		defer odbc.ReleaseHandle(hstmt)
		return nil, odbc.NewError("SQLExecDirect", hstmt)
	}

	var columnCount api.SQLSMALLINT
	ret = api.SQLNumResultCols(hstmt, &columnCount)
	if odbc.IsError(ret) {
		defer odbc.ReleaseHandle(hstmt)
		return nil, odbc.NewError("SQLNumResultCols", hstmt)
	}
	//get column infos in table.
	df.ncols = int(columnCount)
	for i := 0; i < df.ncols; i++ {
		var namelen int
		var sqltype api.SQLSMALLINT
		var size api.SQLULEN
		var l, decimal, nullable api.SQLSMALLINT
		namebuf := make([]uint16, 256)
		ret := api.SQLDescribeCol(hstmt,
			api.SQLUSMALLINT(i+1),
			(*api.SQLWCHAR)(unsafe.Pointer(&namebuf[0])),
			api.SQLSMALLINT(len(namebuf)),
			&l,
			&sqltype,
			&size,
			&decimal,
			&nullable)

		if odbc.IsError(ret) {
			return nil, odbc.NewError("SQLDescribeCol", hstmt)
		}

		namelen = int(l)
		if namelen > len(namebuf) {
			return nil, errors.New("Failed to allocate column name buffer")
		}

		var ci *ColumnInfo
		if len(df.columnInfo) == df.ncols {
			ci = &df.columnInfo[i]
		} else if len(df.columnInfo) < df.ncols {
			ci = &ColumnInfo{column_name: odbc.UTF16ToString(namebuf[0:namelen])}
		}

		switch sqltype {
		case api.SQL_BIT:
			ci.column_c_type = api.SQL_C_BIT
			ci.column_size = 1
		case api.SQL_TINYINT, api.SQL_SMALLINT, api.SQL_INTEGER:
			ci.column_c_type = api.SQL_C_LONG
			ci.column_size = 4
		case api.SQL_BIGINT:
			ci.column_c_type = api.SQL_C_SBIGINT
			ci.column_size = 8
		case api.SQL_FLOAT, api.SQL_REAL, api.SQL_DOUBLE:
			ci.column_c_type = api.SQL_C_DOUBLE
			ci.column_size = 8
		case api.SQL_NUMERIC, api.SQL_DECIMAL:
			ci.column_c_type = api.SQL_C_CHAR
			ci.column_size = int(size)
		case api.SQL_TYPE_TIMESTAMP:
			var v api.SQL_TIMESTAMP_STRUCT
			ci.column_c_type = api.SQL_C_TYPE_TIMESTAMP
			ci.column_size = int(unsafe.Sizeof(v))
		case api.SQL_TYPE_DATE:
			var v api.SQL_DATE_STRUCT
			ci.column_c_type = api.SQL_C_DATE
			ci.column_size = int(unsafe.Sizeof(v))
		case api.SQL_TYPE_TIME:
			var v api.SQL_TIME_STRUCT
			ci.column_c_type = api.SQL_C_TIME
			ci.column_size = int(unsafe.Sizeof(v))

		case api.SQL_GUID:
			var v api.SQLGUID
			ci.column_c_type = api.SQL_C_GUID
			ci.column_size = int(unsafe.Sizeof(v))
		case api.SQL_CHAR, api.SQL_VARCHAR:
			ci.column_c_type = api.SQL_C_CHAR
			// +1 : for null-termination character
			ci.column_size = int(size) + 1
		case api.SQL_WCHAR, api.SQL_WVARCHAR:
			ci.column_c_type = api.SQL_C_WCHAR
			// +1 : for null-termination character
			// *2 : wchars take 2 bytes each
			ci.column_size = (int(size) + 1) * 2
		case api.SQL_BINARY, api.SQL_VARBINARY:
			ci.column_c_type = api.SQL_C_BINARY
			ci.column_size = int(size)
		case api.SQL_LONGVARCHAR:
			ci.column_c_type = api.SQL_C_CHAR
			ci.column_size = 0
		case api.SQL_WLONGVARCHAR, api.SQL_SS_XML:
			ci.column_c_type = api.SQL_C_WCHAR
			ci.column_size = 0
		case api.SQL_LONGVARBINARY:
			ci.column_c_type = api.SQL_C_BINARY
			ci.column_size = 0
		default:
			return nil, fmt.Errorf("unsupported column type %d", sqltype)
		}

		if len(df.columnInfo) < df.ncols {
			df.columnInfo = append(df.columnInfo, *ci)
		}
	}
	return df.columnInfo, nil
}

func (df *DataFrame) columnBinding(conn *odbc.Connection, table string) error {

	var out api.SQLHANDLE

	ret := api.SQLAllocHandle(api.SQL_HANDLE_STMT, conn.Dbc, &out)
	if odbc.IsError(ret) {
		return odbc.NewError("SQLAllocHandle", conn.Dbc)
	}
	hstmt := api.SQLHSTMT(out)
	ret = api.SQLSetStmtUIntPtrAttr(hstmt, api.SQL_ATTR_CONCURRENCY, api.SQL_CONCUR_LOCK, 0)
	if odbc.IsError(ret) {
		return odbc.NewError("SQLSetStmtAttr", hstmt)
	}
	ret = api.SQLSetStmtUIntPtrAttr(hstmt, api.SQL_ATTR_CURSOR_TYPE, api.SQL_CURSOR_KEYSET_DRIVEN, 0)
	if odbc.IsError(ret) {
		return odbc.NewError("SQLSetStmtAttr", hstmt)
	}
	//FIXME
	//mysql error: Optional feature not implemented
	/*
		ret = api.SQLSetStmtUIntPtrAttr(hstmt, api.SQL_ATTR_USE_BOOKMARKS, api.SQL_UB_VARIABLE, 0)
		if odbc.IsError(ret) {
			return odbc.NewError("SQLSetStmtAttr", hstmt)
		}
	*/

	//FIXME
	//PTR CHECK

	fetched_ptr := api.SQLLEN(0)
	ret = api.SQLSetStmtUIntPtrAttr(hstmt, api.SQL_ATTR_ROWS_FETCHED_PTR, uintptr(fetched_ptr), 0)
	if odbc.IsError(ret) {
		return odbc.NewError("SQLSetStmtAttr", hstmt)
	}

	status_ptr := make([]api.SQLUSMALLINT, df.nrows)
	ret = api.SQLSetStmtUIntPtrAttr(hstmt, api.SQL_ATTR_ROW_STATUS_PTR, uintptr(status_ptr[0]), 0)
	if odbc.IsError(ret) {
		return odbc.NewError("SQLSetStmtAttr", hstmt)
	}

	bind_ptr := api.SQLLEN(0)
	ret = api.SQLSetStmtUIntPtrAttr(hstmt, api.SQL_ATTR_ROW_BIND_OFFSET_PTR, uintptr(bind_ptr), 0)
	if odbc.IsError(ret) {
		return odbc.NewError("SQLSetStmtAttr", hstmt)
	}

	ret = api.SQLSetStmtUIntPtrAttr(hstmt, api.SQL_ATTR_ROW_BIND_TYPE, api.SQL_BIND_BY_COLUMN, 0)
	if odbc.IsError(ret) {
		return odbc.NewError("SQLSetStmtAttr", hstmt)
	}

	ret = api.SQLSetStmtUIntPtrAttr(hstmt, api.SQL_ATTR_ROW_ARRAY_SIZE, uintptr(df.nrows), 0)
	if odbc.IsError(ret) {
		return odbc.NewError("SQLSetStmtAttr", hstmt)
	}

	//BIND
	/*
	   SQLRETURN SQLBindCol (
	   SQLHSTMT stmt,
	   SQLSMALLINT col,
	   SQLSMALLINT cType, //buffer type
	   SQLPOINTER value, //buffer ptr
	   SQLLEN max, //buffer size(byte) - only string
	   SQLLEN * valueLength //set string lengths for insert or update - only string
	   );
	*/
	//
	for i, ci := range df.columnInfo {
		columns := df.ColumnSelect(i)
		if len(columns) != df.nrows {
			return errors.New(fmt.Sprintf("dismatch columns size, df.nrows [%d != %d]", len(columns), df.nrows))
		}
		ind := make([]api.SQLLEN, df.nrows)
		switch ci.column_c_type {

		case api.SQL_C_BIT:
			data := make([]byte, df.nrows)
			for j, _ := range data {
				data[j] = columns[j].(byte)
				ind[j] = api.SQLLEN(0)
			}
			ret = api.SQLBindCol(hstmt, api.SQLUSMALLINT(i+1), api.SQL_C_BIT, api.SQLPOINTER(unsafe.Pointer(&data[0])), api.SQLLEN(0), &ind[0])
			if odbc.IsError(ret) {
				return odbc.NewError("SQLBindCol", hstmt)
			}
		case api.SQL_C_LONG:
			data := make([]int32, df.nrows)
			for j, _ := range data {
				data[j] = int32(columns[j].(int))
				ind[j] = api.SQLLEN(0)
			}
			ret = api.SQLBindCol(hstmt, api.SQLUSMALLINT(i+1), api.SQL_C_LONG, api.SQLPOINTER(unsafe.Pointer(&data[0])), api.SQLLEN(0), &ind[0])
			if odbc.IsError(ret) {
				return odbc.NewError("SQLBindCol", hstmt)
			}
		case api.SQL_C_SBIGINT:
			data := make([]int64, df.nrows)
			for j, _ := range data {
				data[j] = int64(columns[j].(int))
				ind[j] = api.SQLLEN(0)
			}
			ret = api.SQLBindCol(hstmt, api.SQLUSMALLINT(i+1), api.SQL_C_SBIGINT, api.SQLPOINTER(unsafe.Pointer(&data[0])), api.SQLLEN(0), &ind[0])
			if odbc.IsError(ret) {
				return odbc.NewError("SQLBindCol", hstmt)
			}
		case api.SQL_C_DOUBLE:
			data := make([]float64, df.nrows)
			for j, _ := range data {
				data[j] = columns[j].(float64)
				ind[j] = api.SQLLEN(0)
			}
			ret = api.SQLBindCol(hstmt, api.SQLUSMALLINT(i+1), api.SQL_C_DOUBLE, api.SQLPOINTER(unsafe.Pointer(&data[0])), api.SQLLEN(0), &ind[0])
			if odbc.IsError(ret) {
				return odbc.NewError("SQLBindCol", hstmt)
			}
		case api.SQL_CHAR:
			var data [][1024]byte
			data = make([][1024]byte, df.nrows)

			for j, _ := range data {
				var strValue string
				if ci.column_go_type == reflect.Float64 {
					strValue = strconv.FormatFloat(columns[j].(float64), 'f', -1, 64)
				} else {
					strValue = columns[j].(string)
				}

				for k, v := range odbc.StringToUTF8(strValue) {
					data[j][k] = v
				}
				ind[j] = api.SQLLEN(api.SQL_NTS)
			}

			ret = api.SQLBindCol(hstmt, api.SQLUSMALLINT(i+1), api.SQL_C_CHAR, api.SQLPOINTER(unsafe.Pointer(&data[0][0])), api.SQLLEN(unsafe.Sizeof(data[0])), &ind[0])
			if odbc.IsError(ret) {
				return odbc.NewError("SQLBindCol", hstmt)
			}
		case api.SQL_WCHAR:
			var data [][1024]uint16
			data = make([][1024]uint16, df.nrows)
			for j, _ := range data {
				//FIXME
				var strValue string
				if ci.column_go_type == reflect.Float64 {
					strValue = strconv.FormatFloat(columns[j].(float64), 'f', -1, 64)
				} else {
					strValue = columns[j].(string)
				}

				for k, v := range odbc.StringToUTF16(strValue) {
					data[j][k] = v
				}
				ind[j] = api.SQLLEN(api.SQL_NTS)
			}

			ret = api.SQLBindCol(hstmt, api.SQLUSMALLINT(i+1), api.SQL_C_WCHAR, api.SQLPOINTER(unsafe.Pointer(&data[0][0])), api.SQLLEN(unsafe.Sizeof(data[0])), &ind[0])
			if odbc.IsError(ret) {
				return odbc.NewError("SQLBindCol", hstmt)
			}
		//Byte String
		case api.SQL_C_BINARY:
			var data [][1024]byte
			data = make([][1024]byte, df.nrows)
			for j, _ := range data {
				for k, v := range odbc.StringToUTF8(columns[j].(string)) {
					data[j][k] = v
				}
				ind[j] = api.SQLLEN(api.SQL_NTS)
			}

			ret = api.SQLBindCol(hstmt, api.SQLUSMALLINT(i+1), api.SQL_C_BINARY, api.SQLPOINTER(unsafe.Pointer(&data[0][0])), api.SQLLEN(unsafe.Sizeof(data[0])), &ind[0])
			if odbc.IsError(ret) {
				return odbc.NewError("SQLBindCol", hstmt)
			}

		case api.SQL_C_TYPE_TIMESTAMP, api.SQL_DATETIME, api.SQL_TIME:
			data := make([]api.SQL_TIMESTAMP_STRUCT, df.nrows)
			for j, _ := range data {
				var timestamp odbc.TimeStamp
				switch v := columns[j].(type) {
				case string:
					//YYYY-MM-DD HH:MM:SS[.fraction]
					//fraction: 소수점 6자리
					var parseTime time.Time
					for _, layout := range odbc.Layouts {
						t, err := time.Parse(layout, v)
						if err == nil {
							parseTime = t
							break
						}
					}
					if parseTime.IsZero() {
						return errors.New("Faild TimeStamp Parsing: " + columns[j].(string))
					}

					timestamp = odbc.GotimeToTimestamp(parseTime)

				case odbc.TimeStamp:
					timestamp = v
				case odbc.Time:
					timestamp = v.ToTimestamp()
				}

				data[j] = timestamp.ToCtimestamp()
				ind[j] = api.SQLLEN(0)
			}

			ret = api.SQLBindCol(hstmt, api.SQLUSMALLINT(i+1), api.SQL_C_TYPE_TIMESTAMP, api.SQLPOINTER(unsafe.Pointer(&data[0])), api.SQLLEN(unsafe.Sizeof(data)), &ind[0])
			if odbc.IsError(ret) {
				return odbc.NewError("SQLBindCol", hstmt)
			}

			/*
				case api.SQL_C_TIME:
					data := make([]api.SQL_TIME_STRUCT, df.nrows)
					for j, _ := range data {
						var timestamp odbc.Time
						switch v := columns[j].(type) {
						case string:
							//YYYY-MM-DD HH:MM:SS[.fraction]
							//fraction: 소수점 6자리
							var parseTime time.Time
							for _, layout := range odbc.Layouts {
								t, err := time.Parse(layout, v)
								if err == nil {
									parseTime = t
									break
								}
							}
							if parseTime.IsZero() {
								return errors.New("Faild TimeStamp Parsing: " + columns[j].(string))
							}
							timestamp = odbc.GotimeToTime(parseTime)
						case odbc.Time:
							timestamp = v
						}
						data[j].Hour = api.SQLUSMALLINT(timestamp.Hour)
						data[j].Minute = api.SQLUSMALLINT(timestamp.Minute)
						data[j].Second = api.SQLUSMALLINT(timestamp.Second)

						ind[j] = api.SQLLEN(0)
					}

					ret = api.SQLBindCol(hstmt, api.SQLUSMALLINT(i+1), api.SQL_C_TIME, api.SQLPOINTER(unsafe.Pointer(&data[0])), api.SQLLEN(0), &ind[0])
					if odbc.IsError(ret) {
						return odbc.NewError("SQLBindCol", hstmt)
					}
			*/
		case api.SQL_C_GUID:
			data := make([]api.SQLGUID, df.nrows)
			ret = api.SQLBindCol(hstmt, api.SQLUSMALLINT(i+1), api.SQL_C_GUID, api.SQLPOINTER(unsafe.Pointer(&data[0])), api.SQLLEN(0), &ind[0])
			if odbc.IsError(ret) {
				return odbc.NewError("SQLBindCol", hstmt)
			}

		}

	}

	query := fmt.Sprintf("SELECT * FROM %s", table)
	b := odbc.StringToUTF16(query)
	ret = api.SQLExecDirect(hstmt, (*api.SQLWCHAR)(unsafe.Pointer(&b[0])), api.SQLINTEGER(len(query)))
	if odbc.IsError(ret) {
		return odbc.NewError("SQLExecDirect", hstmt)
	}

	/*
		ret = api.SQLFetchScroll(hstmt, api.SQL_FETCH_NEXT, api.SQLLEN(0))
		if odbc.IsError(ret) {
			return odbc.NewError("SQLFetchScroll", hstmt)
		}
	*/

	ret = api.SQLBulkOperations(hstmt, api.SQL_ADD)
	if odbc.IsError(ret) {
		return odbc.NewError("SQLBulkOperations", hstmt)
	}

	err := odbc.ReleaseHandle(hstmt)
	if err != nil {
		return err
	}

	return nil

}
