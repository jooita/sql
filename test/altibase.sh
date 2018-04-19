#!/bin/sh

go test -v -dsn TestOdbc -user $user -passwd $passwd -table odbctest

cd altibase
go test -v -dsn TestOdbc -table odbctest
