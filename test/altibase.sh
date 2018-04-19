#!/bin/sh

go test -v -dsn TestOdbc -user keti -passwd $passwd -table odbctest

cd altibase
go test -v -dsn TestOdbc -table odbctest
