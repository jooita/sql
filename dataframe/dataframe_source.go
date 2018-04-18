package dataframe

import "github.com/jooita/sql/odbc"

func (df *DataFrame) ReadODBC(dsn, table string) (*DataFrame, error) {
	df.tableName = table

	conn, err := odbc.Connect(dsn)
	if err != nil {
		return nil, err
	}

	_, err = df.getColumnInfo(conn, table)
	if err != nil {
		return nil, err
	}

	err = df.getColumns(conn, table)
	if err != nil {
		return nil, err
	}

	err = conn.Close()
	if err != nil {
		return nil, err
	}
	return df, nil
}
