#!/bin/sh

#mysql connector/odbc connection parameters
#https://dev.mysql.com/doc/connector-odbc/en/connector-odbc-configuration-connection-parameters.html

go test -v -dsn MySql -user root -passwd ketilinux -table odbctest

cd mysql
go test -v -dsn MySql -table odbctest

