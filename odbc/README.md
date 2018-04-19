ODBC database driver for Go
========
by beji
--------

# Install
<pre><code> go get github.com/jooita/sql </code></pre>

# Example

	package main

	import (
		"github.com/jooita/sql/odbc"
		"log"
	)

	func main() {
		conn, _ := odbc.Connect("DSN=dsn;")                        
		stmt, _ := conn.Prepare("select * from user where username = ?")                
		stmt.Execute("admin")                                                           
		rows, _ := stmt.FetchAll()                                                      
		for i, row := range rows {                                                      
			println(i, row)                                                             
		}                                                                               
		stmt.Close()
		conn.Close()
	}

Tested on:
	Altibase 6.5.1 and CentOS 7.4 (UnixODBC)
	MySQL 5.6.8 and CentOS 7.4 (UnixODBC)
