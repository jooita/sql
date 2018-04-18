package test

import (
	"flag"
	"fmt"
	"os"
)

var (
	dsn    = flag.String("dsn", "", "dsn")
	myuser = flag.String("user", "", "user name")
	mypass = flag.String("passwd", "", "password")
	table  = flag.String("table", "", "table name")
)

func init() {
	flag.Parse()

	required := []string{"dsn", "user", "passwd", "table"}
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
