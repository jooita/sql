package mysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	d "github.com/jooita/sql/dataframe"
	_ "github.com/jooita/sql/driver"
	"github.com/jooita/sql/odbc"
)

func TestInsert_Time(t *testing.T) {
	conn := fmt.Sprintf("DSN=%s;", *dsn)

	db, err := sql.Open("odbc", conn)
	if err != nil {
		t.Fatal(err)
	}

	db.Exec(fmt.Sprintf("drop table %s", *table))
	_, err = db.Exec(fmt.Sprintf("create table %s (id int, timestamp timestamp(6), date date, datetime datetime(6), time time(6))", *table))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(fmt.Sprintf("insert into %s values (1, '1998-12-31 23:59:59', '1998-12-31', '1998-12-31 23:59:59.123456', '23:59:59.123456')", *table))
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("select * from %s", *table))
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		var id int
		var a, b, c, d time.Time
		if rows.Scan(&id, &a, &b, &c, &d); err != nil {
			t.Fatal(err)
		}
		t.Logf("row %d: %v, %v, %v, %v", id, a, b, c, d)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("error closing DB: %v", err)
	}
}

//Mysql
//fractional seconds part of timestamp was ignored
func TestBulkInsert_Time(t *testing.T) {
	conn := fmt.Sprintf("DSN=%s;", *dsn)

	db, err := sql.Open("odbc", conn)
	if err != nil {
		t.Fatal(err)
	}

	db.Exec(fmt.Sprintf("drop table %s", *table))
	_, err = db.Exec(fmt.Sprintf("create table %s (id int, timestamp timestamp(6), date date, datetime datetime(6), time time(6))", *table))
	if err != nil {
		t.Fatal(err)
	}

	df, err := d.NewDataframe().ReadODBC(fmt.Sprintf("DSN=%s", *dsn), *table)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	df.AddRow([]interface{}{1, "2006-01-01 15:04:05", "2006-01-01", "2006-01-01", "15:04:05"})
	df.AddRow([]interface{}{2, "2018-03-03 15:04:05.123456", "2006-01-01", "2018-03-03", "15:04:05.123456"})

	err = df.WriteODBC(fmt.Sprintf("DSN=%s", *dsn), *table, d.Append)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("select * from %s", *table))
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		var id int
		var a, b, c, d time.Time
		if rows.Scan(&id, &a, &b, &c, &d); err != nil {
			t.Fatal(err)
		}
		t.Logf("row %d: %v, %v, %v, %v", id, a, b, c, d)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("error closing DB: %v", err)
	}

}

//Mysql
//fractional seconds part of timestamp was ignored
func TestBulkInsert_TimeStamp(t *testing.T) {

	df := d.NewDataframe(
		d.NewColumnInfo(reflect.Int, "key"),
		d.NewColumnInfo(reflect.Struct, "time"),
		d.NewColumnInfo(reflect.Struct, "time2"),
		d.NewColumnInfo(reflect.Struct, "time3"),
		d.NewColumnInfo(reflect.Struct, "time4"),
	)

	ts := odbc.TimeStamp{Year: 2018, Month: 04, Day: 12, Hour: 9, Minute: 33, Second: 11, Fraction: 123000000}
	date := odbc.TimeStamp{Year: 2018, Month: 04, Day: 12}
	tm1 := odbc.TimeStamp{Year: 2018, Month: 04, Day: 12, Hour: 9}
	tm2 := odbc.Time{Hour: 9, Minute: 33, Second: 11}
	df.AddRow([]interface{}{1, ts, date, tm1, tm2})

	tm3 := odbc.TimeStamp{Year: 2018, Month: 04, Day: 12, Hour: 9, Minute: 33, Second: 11, Fraction: 123456}
	tm4 := odbc.TimeStamp{Hour: 9, Minute: 33, Second: 11, Fraction: 123456000}
	df.AddRow([]interface{}{2, ts, date, tm3, tm4})

	err := df.WriteODBC(fmt.Sprintf("DSN=%s", *dsn), *table, d.Append)
	if err != nil {
		t.Fatal(err)
	}

	conn := fmt.Sprintf("DSN=%s;", *dsn)
	db, err := sql.Open("odbc", conn)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s", *table))
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int
		var a, b, c, d time.Time
		if rows.Scan(&id, &a, &b, &c, &d); err != nil {
			t.Fatal(err)
		}
		t.Logf("row %d: %v, %v, %v, %v", id, a, b, c, d)
	}
	err = db.Close()
	if err != nil {
		t.Fatalf("error closing DB: %v", err)
	}

}
