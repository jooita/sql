#!/bin/sh

go test -v -dsn TestOdbc -user keti -passwd ketilinux -table odbctest

cd altibase
go test -v -dsn TestOdbc -table odbctest
