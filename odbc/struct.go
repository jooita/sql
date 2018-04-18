package odbc

import (
	"time"

	"github.com/jooita/sql/api"
)

type TimeStamp struct {
	Year     int
	Month    int
	Day      int
	Hour     int
	Minute   int
	Second   int
	Fraction int
}

type Time struct {
	Hour   int
	Minute int
	Second int
}

type Date struct {
	Year  int
	Month int
	Day   int
}

var Layouts = []string{
	"2006-01-02 15:04:05.0",
	"2006-01-02 15:04:05.00",
	"2006-01-02 15:04:05.000",
	"2006-01-02 15:04:05.0000",
	"2006-01-02 15:04:05.00000",
	"2006-01-02 15:04:05.000000",
	"2006-01-02 15:04:05.0000000",
	"2006-01-02 15:04:05.00000000",
	"2006-01-02 15:04:05.000000000",
	"2006-01-02 15:04:05",
	"2006-01-02",
	"15:04:05.0",
	"15:04:05.00",
	"15:04:05.000",
	"15:04:05.0000",
	"15:04:05.00000",
	"15:04:05.000000",
	"15:04:05.0000000",
	"15:04:05.00000000",
	"15:04:05.000000000",
	"15:04:05",
}

func (t Time) ToTimestamp() (data TimeStamp) {
	data.Hour = t.Hour
	data.Minute = t.Minute
	data.Second = t.Second
	return
}

func (timestamp TimeStamp) ToCtimestamp() (data api.SQL_TIMESTAMP_STRUCT) {
	data.Year = api.SQLSMALLINT(timestamp.Year)
	data.Month = api.SQLUSMALLINT(timestamp.Month)
	data.Day = api.SQLUSMALLINT(timestamp.Day)
	data.Hour = api.SQLUSMALLINT(timestamp.Hour)
	data.Minute = api.SQLUSMALLINT(timestamp.Minute)
	data.Second = api.SQLUSMALLINT(timestamp.Second)
	data.Fraction = api.SQLUINTEGER(timestamp.Fraction)
	return
}

func (timestamp TimeStamp) ToCtime() (data api.SQL_TIME_STRUCT) {
	data.Hour = api.SQLUSMALLINT(timestamp.Hour)
	data.Minute = api.SQLUSMALLINT(timestamp.Minute)
	data.Second = api.SQLUSMALLINT(timestamp.Second)
	return
}

func GotimeToTimestamp(parseTime time.Time) TimeStamp {
	timestamp := TimeStamp{}
	timestamp.Year = (parseTime.Year())
	timestamp.Month = int(parseTime.Month())
	timestamp.Day = (parseTime.Day())
	timestamp.Hour = (parseTime.Hour())
	timestamp.Minute = (parseTime.Minute())
	timestamp.Second = (parseTime.Second())
	timestamp.Fraction = (parseTime.Nanosecond())
	return timestamp
}

func GotimeToTime(parseTime time.Time) Time {
	timestamp := Time{}
	timestamp.Hour = (parseTime.Hour())
	timestamp.Minute = (parseTime.Minute())
	timestamp.Second = (parseTime.Second())
	//timestamp.Fraction = (parseTime.Nanosecond())
	return timestamp
}
