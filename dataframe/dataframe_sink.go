package dataframe

import "fmt"
import "github.com/jooita/sql/odbc"

type mode int

const (
	//if data/table already exists, contents of the DataFrame are expected to be appended to existing data.
	Append mode = iota
	//if data/table already exists, existing data is expected to be overwritten by the contents of the DataFrame.
	Overwrite
	//if data already exists, the save operation is expected to not save the contents of the DataFrame and to not change the existing data. This is similar to a CREATE TABLE IF NOT EXISTS in SQL.
	Ignore
	//if data already exists, an exception is expected to be thrown.
	ErrorIfExists
)

func (df *DataFrame) WriteODBC(dsn, table string, savemode mode) error {
	conn, err := odbc.Connect(dsn)
	if err != nil {
		return err
	}
	_, err = df.getColumnInfo(conn, table)
	if err != nil {
		return err
	}

	switch savemode {
	case Append:
		err = df.columnBinding(conn, table)
		if err != nil {
			return err
		}
		err = conn.Close()
		if err != nil {
			return err
		}
		return nil
	case Overwrite:
		_, err = conn.ExecDirect(fmt.Sprintf("Delete from %s", table))
		if err != nil {
			return err
		}
		err = df.columnBinding(conn, table)
		if err != nil {
			return err
		}
		err = conn.Close()
		if err != nil {
			return err
		}
		return nil
	case Ignore:
		stmt, err := conn.ExecDirect(fmt.Sprintf("Select * from %s", table))
		if err != nil {
			return err
		}
		numrows, err := stmt.NumRows()
		if err != nil {
			return err
		}

		if numrows != 0 {
			return nil
		}

		err = df.columnBinding(conn, table)
		if err != nil {
			return err
		}
		err = conn.Close()
		if err != nil {
			return err
		}
		return nil
	case ErrorIfExists:

	}

	return nil
}
