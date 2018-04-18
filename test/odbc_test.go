package test

import (
	"github.com/jooita/sql/odbc"
	"fmt"
	"testing"
)

func TestOdbc(t *testing.T) {
	dsn := fmt.Sprintf("DSN=%s;UID=%s;PWD=%s;", *dsn, *myuser, *mypass)

	conn, err := odbc.Connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.Prepare(fmt.Sprintf("select * from %s", *table))
	if err != nil {
		t.Fatal(err)
	}
	err = stmt.Execute()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := stmt.FetchAll()
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range rows {
		t.Logf("row %d - %v", i, row.Data)
	}
	stmt.Close()
	conn.Close()
}
