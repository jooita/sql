// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

//go:generate go run $GOROOT/src/syscall/mksyscall_windows.go -output zapi_windows.go api.go

//go:generate sh -c "./mksyscall_unix.pl api.go | gofmt > zapi_unix.go"

type (
	SQL_DATE_STRUCT struct {
		Year  SQLSMALLINT
		Month SQLUSMALLINT
		Day   SQLUSMALLINT
	}

	SQL_TIME_STRUCT struct {
		Hour   SQLUSMALLINT
		Minute SQLUSMALLINT
		Second SQLUSMALLINT
	}

	SQL_TIMESTAMP_STRUCT struct {
		Year     SQLSMALLINT
		Month    SQLUSMALLINT
		Day      SQLUSMALLINT
		Hour     SQLUSMALLINT
		Minute   SQLUSMALLINT
		Second   SQLUSMALLINT
		Fraction SQLUINTEGER
	}
)

//sys	SQLAllocHandle(handleType SQLSMALLINT, inputHandle SQLHANDLE, outputHandle *SQLHANDLE) (ret SQLRETURN) = odbc32.SQLAllocHandle
//sys	SQLBindCol(statementHandle SQLHSTMT, columnNumber SQLUSMALLINT, targetType SQLSMALLINT, targetValuePtr SQLPOINTER, bufferLength SQLLEN, vallen *SQLLEN) (ret SQLRETURN) = odbc32.SQLBindCol
//sys	SQLBindParameter(statementHandle SQLHSTMT, parameterNumber SQLUSMALLINT, inputOutputType SQLSMALLINT, valueType SQLSMALLINT, parameterType SQLSMALLINT, columnSize SQLULEN, decimalDigits SQLSMALLINT, parameterValue SQLPOINTER, bufferLength SQLLEN, ind *SQLLEN) (ret SQLRETURN) = odbc32.SQLBindParameter
//sys	SQLCloseCursor(statementHandle SQLHSTMT) (ret SQLRETURN) = odbc32.SQLCloseCursor
//sys	SQLDescribeCol(statementHandle SQLHSTMT, columnNumber SQLUSMALLINT, columnName *SQLWCHAR, bufferLength SQLSMALLINT, nameLengthPtr *SQLSMALLINT, dataTypePtr *SQLSMALLINT, columnSizePtr *SQLULEN, decimalDigitsPtr *SQLSMALLINT, nullablePtr *SQLSMALLINT) (ret SQLRETURN) = odbc32.SQLDescribeColW
//sys	SQLDescribeParam(statementHandle SQLHSTMT, parameterNumber SQLUSMALLINT, dataTypePtr *SQLSMALLINT, parameterSizePtr *SQLULEN, decimalDigitsPtr *SQLSMALLINT, nullablePtr *SQLSMALLINT) (ret SQLRETURN) = odbc32.SQLDescribeParam
//sys	SQLDisconnect(connectionHandle SQLHDBC) (ret SQLRETURN) = odbc32.SQLDisconnect
//sys	SQLDriverConnect(connectionHandle SQLHDBC, windowHandle SQLHWND, inConnectionString *SQLWCHAR, stringLength1 SQLSMALLINT, outConnectionString *SQLWCHAR, bufferLength SQLSMALLINT, stringLength2Ptr *SQLSMALLINT, driverCompletion SQLUSMALLINT) (ret SQLRETURN) = odbc32.SQLDriverConnectW
//sys	SQLEndTran(handleType SQLSMALLINT, handle SQLHANDLE, completionType SQLSMALLINT) (ret SQLRETURN) = odbc32.SQLEndTran
//sys	SQLExecute(statementHandle SQLHSTMT) (ret SQLRETURN) = odbc32.SQLExecute
//sys	SQLFetch(statementHandle SQLHSTMT) (ret SQLRETURN) = odbc32.SQLFetch
//sys	SQLFreeHandle(handleType SQLSMALLINT, handle SQLHANDLE) (ret SQLRETURN) = odbc32.SQLFreeHandle
//sys	SQLGetData(statementHandle SQLHSTMT, colOrParamNum SQLUSMALLINT, targetType SQLSMALLINT, targetValuePtr SQLPOINTER, bufferLength SQLLEN, vallen *SQLLEN) (ret SQLRETURN) = odbc32.SQLGetData
//sys	SQLGetDiagRec(handleType SQLSMALLINT, handle SQLHANDLE, recNumber SQLSMALLINT, sqlState *SQLWCHAR, nativeErrorPtr *SQLINTEGER, messageText *SQLWCHAR, bufferLength SQLSMALLINT, textLengthPtr *SQLSMALLINT) (ret SQLRETURN) = odbc32.SQLGetDiagRecW
//sys	SQLNumParams(statementHandle SQLHSTMT, parameterCountPtr *SQLSMALLINT) (ret SQLRETURN) = odbc32.SQLNumParams
//sys	SQLNumResultCols(statementHandle SQLHSTMT, columnCountPtr *SQLSMALLINT)  (ret SQLRETURN) = odbc32.SQLNumResultCols
//sys	SQLPrepare(statementHandle SQLHSTMT, statementText *SQLWCHAR, textLength SQLINTEGER) (ret SQLRETURN) = odbc32.SQLPrepareW
//sys	SQLRowCount(statementHandle SQLHSTMT, rowCountPtr *SQLLEN) (ret SQLRETURN) = odbc32.SQLRowCount
//sys	SQLSetEnvAttr(environmentHandle SQLHENV, attribute SQLINTEGER, valuePtr SQLPOINTER, stringLength SQLINTEGER) (ret SQLRETURN) = odbc32.SQLSetEnvAttr
//sys	SQLSetConnectAttr(connectionHandle SQLHDBC, attribute SQLINTEGER, valuePtr SQLPOINTER, stringLength SQLINTEGER) (ret SQLRETURN) = odbc32.SQLSetConnectAttrW
//sys	SQLSetStmtAttr(statementHandle SQLHSTMT, attribute SQLINTEGER, valuePtr SQLPOINTER, stringLength SQLINTEGER) (ret SQLRETURN) = odbc32.SQLSetStmtAttrW
//sys	SQLExecDirect(statementHandle SQLHSTMT, statementText *SQLWCHAR, textLength SQLINTEGER) (ret SQLRETURN) = odbc32.SQLExecDirectW
//sys	SQLColAttribute(statementHandle SQLHSTMT, columnNumber SQLUSMALLINT, fieldIdentifier SQLUSMALLINT, characterAttributePtr SQLPOINTER, bufferLength SQLSMALLINT, stringLengthPtr *SQLSMALLINT, numericAttributePtr *SQLLEN) (ret SQLRETURN) = odbc32.SQLColAttribute
//sys	SQLGetInfo(connectionHandle SQLHDBC, infoType SQLUSMALLINT, infoValuePtr SQLPOINTER, bufferLength SQLSMALLINT, stringLengthPtr *SQLSMALLINT) (ret SQLRETURN) = odbc32.SQLGetInfo
//sys	SQLCancel(statementHandle SQLHSTMT) (ret SQLRETURN) = odbc32.SQLCancel
//sys	SQLMoreResults(statementHandle SQLHSTMT) (ret SQLRETURN) = odbc32.SQLMoreResults
//sys	SQLBulkOperations(statementHandle SQLHSTMT, operation SQLSMALLINT) (ret SQLRETURN) = odbc32.SQLBulkOperations
//sys	SQLFetchScroll(statementHandle SQLHSTMT, fetchOrientation SQLSMALLINT, fetchOffset SQLLEN) (ret SQLRETURN) = odbc32.SQLFetchScroll
