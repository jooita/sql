package dataframe

import (
	"reflect"

	"github.com/jooita/sql/api"
)

type ColumnInfo struct {
	column_size    int
	column_go_type reflect.Kind
	column_c_type  api.SQLSMALLINT
	column_name    string
}

func NewColumnInfo(gotype reflect.Kind, name string) ColumnInfo {
	return ColumnInfo{column_name: name, column_go_type: gotype}
}
