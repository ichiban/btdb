package sql

import "database/sql/driver"

func namedValues(args []driver.Value) []driver.NamedValue {
	vs := make([]driver.NamedValue, len(args))
	for i, a := range args {
		vs[i] = driver.NamedValue{
			Ordinal: i,
			Value:   a,
		}
	}
	return vs
}
