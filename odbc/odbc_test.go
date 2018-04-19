package odbc

import (
	//	"github.com/jooita/sql/odbc"
	"flag"
	"fmt"
	"os"
	"testing"
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

func TestOdbc(t *testing.T) {
	dsn := fmt.Sprintf("DSN=%s;", *dsn)

	conn, err := Connect(dsn)
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
