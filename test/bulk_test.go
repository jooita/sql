package test

import (
	"database/sql"
	"fmt"
	"testing"

	d "github.com/jooita/sql/dataframe"
	_ "github.com/jooita/sql/driver"
)

func TestBulkInsert_Integer(t *testing.T) {
	conn := fmt.Sprintf("DSN=%s;", *dsn)

	db, err := sql.Open("odbc", conn)
	if err != nil {
		t.Fatal(err)
	}

	db.Exec(fmt.Sprintf("drop table %s", *table))
	_, err = db.Exec(fmt.Sprintf("create table %s (a INTEGER, b SMALLINT, c BIGINT)", *table))
	if err != nil {
		t.Fatal(err)
	}

	df, _ := d.NewDataframe().ReadODBC(fmt.Sprintf("DSN=%s", *dsn), *table)
	df.AddRow([]interface{}{1, 2, 3})
	df.AddRow([]interface{}{123122, 2, 123211232})
	df.AddRow([]interface{}{-3, -2, -123211232})

	err = df.WriteODBC(fmt.Sprintf("DSN=%s", *dsn), *table, d.Append)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("select * from %s", *table))
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		var a, b, c int
		if rows.Scan(&a, &b, &c); err != nil {
			t.Fatal(err)
		}
		t.Log(a, b, c)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("error closing DB: %v", err)
	}

}

func TestBulkInsert_Real(t *testing.T) {
	conn := fmt.Sprintf("DSN=%s;", *dsn)

	db, err := sql.Open("odbc", conn)
	if err != nil {
		t.Fatal(err)
	}

	db.Exec(fmt.Sprintf("drop table %s", *table))

	// in altibase.
	// NUMERIC, DECIMAL -> NUMERIC
	// NUMBER, FLOAT -> FLOAT
	//_, err = db.Exec(fmt.Sprintf("create table %s (a NUMERIC(20,11), b DECIMAL(20,11), c NUMBER, d FLOAT, e DOUBLE, f REAL)", *table))
	_, err = db.Exec(fmt.Sprintf("create table %s (a NUMERIC(20,11), b DECIMAL(20,11), c FLOAT, d FLOAT, e DOUBLE, f REAL)", *table))
	if err != nil {
		t.Fatal(err)
	}

	df, _ := d.NewDataframe().ReadODBC(fmt.Sprintf("DSN=%s", *dsn), *table)
	df.AddRow([]interface{}{4.4, 5.5, 6.6, 7.7, 8.8, 9.9})
	df.AddRow([]interface{}{12.1234567, 12.1234567, 99.99, 999.999, 9999.9999, 999.999})

	err = df.WriteODBC(fmt.Sprintf("DSN=%s", *dsn), *table, d.Append)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("select * from %s", *table))
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		//var a, b, c, d, e, f float64
		var a, b, c, d, e, f interface{}
		if rows.Scan(&a, &b, &c, &d, &e, &f); err != nil {
			t.Fatal(err)
		}
		t.Log(a, b, c, d, e, f)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("error closing DB: %v", err)
	}

}
func TestBulkInsert_String(t *testing.T) {
	conn := fmt.Sprintf("DSN=%s;", *dsn)

	db, err := sql.Open("odbc", conn)
	if err != nil {
		t.Fatal(err)
	}

	db.Exec(fmt.Sprintf("drop table %s", *table))
	_, err = db.Exec(fmt.Sprintf("create table %s (a VARCHAR(20), b CHAR(20), c NCHAR(2), d NVARCHAR(5))", *table))
	if err != nil {
		t.Fatal(err)
	}

	df, _ := d.NewDataframe().ReadODBC(fmt.Sprintf("DSN=%s", *dsn), *table)

	df.AddRow([]interface{}{"a", "b", "c", "d"})
	df.AddRow([]interface{}{"aa", "bb", "cc", "dd"})
	df.AddRow([]interface{}{"aaaaaaaaaaaaaaaaaaa", "bbbbbbbbb", "cc", "ddddd"})
	df.AddRow([]interface{}{"aaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbb", "cc", "ddddd"})

	err = df.WriteODBC(fmt.Sprintf("DSN=%s", *dsn), *table, d.Append)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("select * from %s", *table))
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		var a, b, c, d string
		if rows.Scan(&a, &b, &c, &d); err != nil {
			t.Fatal(err)
		}
		t.Log(a, b, c, d)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("error closing DB: %v", err)
	}

}

func TestBulkInsert_ReadODBC(t *testing.T) {
	df, _ := d.NewDataframe().ReadODBC(fmt.Sprintf("DSN=%s", *dsn), *table)
	results, err := df.Print()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\n%s", results)

}
