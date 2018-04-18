package dataframe

import "C"
import (
	"fmt"
	"reflect"
)

type DataFrame struct {
	columnInfo []ColumnInfo
	columns    [][]interface{}
	ncols      int
	nrows      int
	tableName  string
}

func NewDataframe(columninfos ...ColumnInfo) *DataFrame {
	df := DataFrame{}
	df.columns = make([][]interface{}, 0)

	df.columnInfo = columninfos
	df.ncols = len(columninfos)
	return &df
}

func (df *DataFrame) Clear() {
	df.columns = make([][]interface{}, 0)
	df.nrows = 0
}

func (df *DataFrame) ColumnSelect(index int) []interface{} {
	//ret := make([]interface{}, df.nrows)
	ret := make([]interface{}, 0)
	for _, v := range df.columns {
		ret = append(ret, v[index])
	}
	return ret
}

func (df *DataFrame) RowSelect(index int) []interface{} {
	return df.columns[index]
}

func (df *DataFrame) AddRow(args []interface{}) {
	if df.columnInfo == nil {
		fmt.Printf("Need to ColumnInfo.\n")
		panic("")
	}
	if len(args) != df.ncols {
		fmt.Printf("Column number and Row length are mismatch, %d != %d\n", len(args), df.ncols)
		panic("")
	}

	df.columns = append(df.columns, args)
	df.nrows++

	for i, ci := range df.columnInfo {
		if ci.column_go_type != reflect.Invalid {
			if ci.column_go_type != reflect.TypeOf(args[i]).Kind() {
				fmt.Printf("column type are mismatch, %v != %v\n", ci.column_go_type, reflect.TypeOf(args[i]))
				panic("")
			}
		}
		df.columnInfo[i].column_go_type = reflect.TypeOf(args[i]).Kind()
	}
}

func (df *DataFrame) SetTableName(name string) {
	df.tableName = name
}

func (df *DataFrame) Close() {
}
