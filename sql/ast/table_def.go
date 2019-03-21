package ast

type TableDefinition struct {
	Scope      *TableScope
	Name       string
	Columns    []ColumnDefinition
	PrimaryKey []string
	UniqueKeys [][]string
}

type TableScope int

const (
	GlobalTableScope TableScope = iota
	LocalTableScope
)

type ColumnDefinition struct {
	Name     string
	DataType DataType
}

type DataType int

const (
	Text DataType = iota
	Integer
)
