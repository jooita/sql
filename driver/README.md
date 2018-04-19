ODBC Driver
========
Implements database driver interface as used by standard database/sql package
by beji

# Install
<pre><code> go get github.com/jooita/sql </code></pre>

# Example

	package main
	import (
		_ "github.com/jooita/sql/driver"
		"database/sql"
		"log"
	)
	func main() {

		db, err := sql.Open("odbc", "DSN=Test;")
		defer db.Close()
		if err != nil {
			log.Fatal(err)
		}

		stmt, err := db.Prepare("select id, time from table")
		defer stmt.Close()

		rows, err := stmt.Query()
		defer rows.Close()
		if err != nil {                        
			log.Fatal(err)                             
		}                                           
												   
		for rows.Next() {
			var id int
			var time interface{}
			if rows.Scan(&id, &time); err != nil {
				log.Fatal(err)                   
			}                                   
			fmt.Println(id, time)
		}
	}

# Tested on
	Altibase 6.5.1 and CentOS 7.4 (UnixODBC)
    MySQL 5.6.8 and CentOS 7.4 (UnixODBC)
