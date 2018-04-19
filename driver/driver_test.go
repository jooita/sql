package driver

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"testing"
	//_ "github.com/jooita/sql/driver"
)

var (
	dsn   = flag.String("dsn", "", "dsn")
	table = flag.String("table", "", "table name")
)

func init() {
	flag.Parse()

	required := []string{"dsn", "table"}
	seen := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	for _, req := range required {
		if !seen[req] {
			// or possibly use `log.Fatalf` instead of:
			fmt.Fprintf(os.Stderr, "missing required -%s argument(flag)\n", req)
			os.Exit(2) // the same exit code flag.Parse uses
		}
	}
}

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
