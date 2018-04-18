package mysql

import (
	"flag"
	"fmt"
	"os"
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
