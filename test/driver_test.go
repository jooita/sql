package test

import (
	_ "github.com/jooita/sql/driver"
	"database/sql"
	"fmt"
	"testing"
)

func TestDriverOdbc(t *testing.T) {

	dsn := fmt.Sprintf("DSN=%s;", *dsn)
	db, err := sql.Open("odbc", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stmt, err := db.Prepare(fmt.Sprintf("select * from %s", *table))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		ptr := make([]interface{}, len(columns))
		row := make([]interface{}, len(columns))
		for i, _ := range ptr {
			ptr[i] = &row[i]
		}

		err := rows.Scan(ptr...)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(row)
	}
}
