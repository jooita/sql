package dataframe

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

const (
	empty               = ""
	space               = " "
	plus                = "+"
	minus               = "-"
	pipe                = "|"
	newLine             = "\n"
	intString           = "int"
	floatString         = "float"
	leftJustification   = "LEFT"
	rightJustification  = "RIGHT"
	sqlRowFooter        = "(1 row)"
	sqlRowsFooterFormat = "(%d rows)"
)

// Pretty returns a pretty sql string
func (df *DataFrame) Print() (string, error) {

	if df.columnInfo == nil {
		return empty, errors.New("Empty column info.")
	}

	columnNames := make([]string, df.ncols)
	columnSizes := make([]int, df.ncols)
	columnJustifications := make([]string, df.ncols)
	for i, v := range df.columnInfo {

		columnNames[i] = v.column_name
		columnSizes[i] = len(v.column_name)
		kind := v.column_go_type.String()
		if strings.Contains(kind, intString) || strings.Contains(kind, floatString) {
			columnJustifications[i] = rightJustification
		} else {
			columnJustifications[i] = leftJustification
		}

	}

	if len(columnNames) == 0 {
		return getFooter(0), nil
	}

	values := make([][]string, df.nrows)
	for i := 0; i < df.nrows; i++ {
		value := make([]string, df.ncols)
		row := df.RowSelect(i)
		for j := 0; j < df.ncols; j++ {
			value[j] = fmt.Sprintf("%v", row[j])
			if columnSizes[j] < len(value[j]) {
				columnSizes[j] = len(value[j])
			}
		}
		values[i] = value
	}

	header := getHeader(columnNames, columnSizes)
	body := getBody(values, columnSizes, columnJustifications)
	footer := getFooter(len(values))

	var results bytes.Buffer
	results.WriteString(header)
	results.WriteString(body)
	results.WriteString(footer)
	return results.String(), nil
}

func getHeader(names []string, sizes []int) string {
	var header bytes.Buffer
	header.WriteString(space)
	header.WriteString(Center(names[0], sizes[0]))
	header.WriteString(space)

	for i := 1; i < len(names); i++ {
		header.WriteString(pipe)
		header.WriteString(space)
		header.WriteString(Center(names[i], sizes[i]))
		header.WriteString(space)
	}

	header.WriteString(newLine)
	for i := 0; i < 2+sizes[0]; i++ {
		header.WriteString(minus)
	}
	for i := 1; i < len(names); i++ {
		header.WriteString(plus)
		for j := 0; j < 2+sizes[i]; j++ {
			header.WriteString(minus)
		}
	}
	header.WriteString(newLine)
	return header.String()
}

func getBody(values [][]string, columnSizes []int, columnJustifications []string) string {
	var body bytes.Buffer
	for rowIndex := 0; rowIndex < len(values); rowIndex++ {
		body.WriteString(space)
		switch columnJustifications[0] {
		case leftJustification:
			body.WriteString(Left(values[rowIndex][0], columnSizes[0]))
		case rightJustification:
			body.WriteString(Right(values[rowIndex][0], columnSizes[0]))
		default:
			body.WriteString(Center(values[rowIndex][0], columnSizes[0]))
		}
		body.WriteString(space)

		for columnIndex := 1; columnIndex < len(values[rowIndex]); columnIndex++ {
			body.WriteString(pipe)
			body.WriteString(space)
			switch columnJustifications[columnIndex] {
			case leftJustification:
				body.WriteString(Left(values[rowIndex][columnIndex], columnSizes[columnIndex]))
			case rightJustification:
				body.WriteString(Right(values[rowIndex][columnIndex], columnSizes[columnIndex]))
			default:
				body.WriteString(Center(values[rowIndex][columnIndex], columnSizes[columnIndex]))
			}
			body.WriteString(space)
		}
		body.WriteString(newLine)
	}
	return body.String()
}

func getFooter(rowCount int) string {
	if rowCount == 1 {
		return sqlRowFooter
	}
	return fmt.Sprintf(sqlRowsFooterFormat, rowCount)
}

// Left justifies the text to the left
func Left(text string, size int) string {
	spaces := size - Length(text)
	if spaces <= 0 {
		return text
	}

	var buffer bytes.Buffer
	buffer.WriteString(text)

	for i := 0; i < spaces; i++ {
		buffer.WriteString(space)
	}
	return buffer.String()
}

// Right justifies the text to the right
func Right(text string, size int) string {
	spaces := size - Length(text)
	if spaces <= 0 {
		return text
	}

	var buffer bytes.Buffer
	for i := 0; i < spaces; i++ {
		buffer.WriteString(space)
	}

	buffer.WriteString(text)
	return buffer.String()
}

func Center(text string, size int) string {
	left := Right(text, (Length(text)+size)/2)
	return Left(left, size)
}

func Length(text string) int {
	textRunes := []rune(text)
	textRunesLength := len(textRunes)

	sum, i, j := 0, 0, 0
	for i < textRunesLength && j < textRunesLength {
		j = i + 1
		for j < textRunesLength && IsMark(textRunes[j]) {
			j++
		}
		sum++
		i = j
	}
	return sum
}

func IsMark(r rune) bool {
	return unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r) || unicode.Is(unicode.Mc, r)
}
