package sql

import (
	"database/sql/driver"

	"golang.org/x/xerrors"
)

type Parser struct {
	store Store
	lex   *Lexer
	token token
}

func NewParser(s Store, input string) *Parser {
	l := NewLexer(input)
	go l.Run()
	return &Parser{
		store: s,
		lex:   l,
		token: l.Next(),
	}
}

var ErrIncomplete = xerrors.New("incomplete statement")

func (p *Parser) accept(typ tokenType) (interface{}, error) {
	if p.token.typ == eos {
		return nil, ErrIncomplete
	}
	if p.token.typ != typ {
		return nil, xerrors.Errorf("expected: %s, got: %s", typ, p.token.typ)
	}
	val := p.token.val
	p.next()
	return val, nil
}

func (p *Parser) next() {
	p.token = p.lex.Next()
}

func (p *Parser) DirectSQLStatement() (driver.Stmt, error) {
	stmt, err := p.directlyExecutableStatement()
	if err != nil {
		return nil, xerrors.Errorf("while parsing directly executable statement: %w", err)
	}
	if _, err := p.accept(semicolon); err != nil {
		return nil, xerrors.Errorf("while parsing directly executable statement: %w", err)
	}
	return stmt, nil
}

func (p *Parser) directlyExecutableStatement() (driver.Stmt, error) {
	switch p.token.typ {
	case kwSelect, kwInsert, kwUpdate:
		return p.directSQLDataStatement()
	case kwCreate:
		return p.sqlSchemaStatement()
	default:
		return nil, xerrors.New("neither direct SQL data statement nor SQL schema statement")
	}
}

func (p *Parser) directSQLDataStatement() (driver.Stmt, error) {
	s, err := p.directSelectStatement()
	if err != nil {
		if xerrors.Is(err, ErrIncomplete) {
			return nil, err
		}
		switch p.token.typ {
		case kwInsert:
			return p.insertStatement()
		case kwUpdate:
			return nil, xerrors.New("not implemented") // TODO
		default:
			return nil, xerrors.New("neither insert nor update")
		}
	}
	return s, nil
}

func (p *Parser) directSelectStatement() (driver.Stmt, error) {
	return p.cursorSpecification()
}

func (p *Parser) cursorSpecification() (driver.Stmt, error) {
	return p.queryExpression()
}

func (p *Parser) queryExpression() (driver.Stmt, error) {
	return p.queryExpressionBody()
}

func (p *Parser) queryExpressionBody() (driver.Stmt, error) {
	return p.queryTerm()
}

func (p *Parser) queryTerm() (driver.Stmt, error) {
	return p.queryPrimary()
}

func (p *Parser) queryPrimary() (driver.Stmt, error) {
	return p.simpleTable()
}

func (p *Parser) simpleTable() (driver.Stmt, error) {
	return p.querySpecification()
}

func (p *Parser) querySpecification() (driver.Stmt, error) {
	if _, err := p.accept(kwSelect); err != nil {
		return nil, err
	}
	if _, err := p.selectList(); err != nil {
		return nil, err
	}
	q, err := p.tableExpression()
	if err != nil {
		return nil, err
	}
	return q, nil
}

func (p *Parser) tableExpression() (driver.Stmt, error) {
	s, err := p.fromClause()
	if err != nil {
		return nil, err
	}
	return &SelectStatement{From: s}, nil
}

func (p *Parser) fromClause() (string, error) {
	if _, err := p.accept(kwFrom); err != nil {
		return "", err
	}
	return p.tableReferenceList()
}

func (p *Parser) tableReferenceList() (string, error) {
	return p.tableReference()
}

func (p *Parser) tableReference() (string, error) {
	return p.tableFactor()
}

func (p *Parser) tableFactor() (string, error) {
	return p.tablePrimary()
}

func (p *Parser) tablePrimary() (string, error) {
	return p.tableOrQueryName()
}

func (p *Parser) tableOrQueryName() (string, error) {
	return p.tableName()
}

func (p *Parser) tableName() (string, error) {
	return p.localOrSchemaQualifiedName()
}

func (p *Parser) localOrSchemaQualifiedName() (string, error) {
	return p.qualifiedIdentifier()
}

func (p *Parser) qualifiedIdentifier() (string, error) {
	v, err := p.accept(identifier)
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

func (p *Parser) selectList() ([]string, error) {
	if _, err := p.accept(asterisk); err != nil {
		return nil, err
	}
	return nil, nil
}

func (p *Parser) insertStatement() (driver.Stmt, error) {
	var s InsertStatement

	if _, err := p.accept(kwInsert); err != nil {
		return nil, xerrors.Errorf("while parsing insert statement: %w", err)
	}
	if _, err := p.accept(kwInto); err != nil {
		return nil, xerrors.Errorf("while parsing insert statement: %w", err)
	}
	name, err := p.insertionTarget()
	if err != nil {
		return nil, xerrors.Errorf("while parsing insert statement: %w", err)
	}
	s.Target = name
	source, err := p.insertColumnsAndSource()
	if err != nil {
		return nil, xerrors.Errorf("while parsing insert statement: %w", err)
	}
	s.Source = source
	return &s, nil
}

func (p *Parser) insertionTarget() (string, error) {
	val, err := p.accept(identifier)
	if err != nil {
		return "", err
	}
	return val.(string), nil
}

func (p *Parser) insertColumnsAndSource() (Source, error) {
	if s, err := p.fromDefault(); err == nil {
		return s, nil
	}
	if s, err := p.fromSubquery(); err == nil {
		return s, nil
	}
	return p.fromConstructor()
}

func (p *Parser) fromSubquery() (Source, error) {
	return nil, xerrors.New("not implemented") // TODO
}

func (p *Parser) fromConstructor() (Source, error) {
	if _, err := p.accept(leftParen); err == nil {
		_, err := p.insertColumnList()
		if err != nil {
			return nil, err
		}
		if _, err := p.accept(rightParen); err != nil {
			return nil, err
		}
	}

	v, err := p.contextuallyTypedTableValueConstructor()
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (p *Parser) insertColumnList() ([]string, error) {
	return p.columnNameList()
}

func (p *Parser) contextuallyTypedTableValueConstructor() (Source, error) {
	if _, err := p.accept(kwValues); err != nil {
		return nil, err
	}
	return p.contextuallyTypedRowValueExpressionList()
}

func (p *Parser) contextuallyTypedRowValueExpressionList() (Source, error) {
	var s FromConstructorSource
	v, err := p.contextuallyTypedRowValueExpression()
	if err != nil {
		return nil, xerrors.Errorf("while parsing the first contextually typed row value expression: %w", err)
	}
	s = append(s, v)
	for {
		if _, err := p.accept(comma); err != nil {
			break
		}
		v, err := p.contextuallyTypedRowValueExpression()
		if err != nil {
			return nil, xerrors.Errorf("while parsing a contextually typed row value expression: %w", err)
		}
		s = append(s, v)
	}
	return &s, nil
}

func (p *Parser) contextuallyTypedRowValueExpression() ([]interface{}, error) {
	return p.contextuallyTypedRowValueConstructor()
}

func (p *Parser) contextuallyTypedRowValueConstructor() ([]interface{}, error) {
	if _, err := p.accept(leftParen); err != nil {
		return nil, err
	}
	var vs []interface{}
	for {
		v, err := p.contextuallyTypedRowValueConstructorElement()
		if err != nil {
			return nil, err
		}
		vs = append(vs, v)
		if _, err := p.accept(comma); err != nil {
			break
		}
	}
	if _, err := p.accept(rightParen); err != nil {
		return nil, err
	}
	return vs, nil
}

func (p *Parser) contextuallyTypedRowValueConstructorElement() (interface{}, error) {
	return p.valueExpression()
}

func (p *Parser) valueExpression() (interface{}, error) {
	return p.commonValueExpression()
}

func (p *Parser) commonValueExpression() (interface{}, error) {
	if v, err := p.numericValueExpression(); err == nil {
		return v, nil
	}
	if v, err := p.stringValueExpression(); err == nil {
		return v, nil
	}
	return nil, xerrors.New("non common value expression")
}

func (p *Parser) numericValueExpression() (interface{}, error) {
	return p.term()
}

func (p *Parser) term() (interface{}, error) {
	return p.factor()
}

func (p *Parser) factor() (interface{}, error) {
	_, _ = p.sign() // TODO
	return p.numericPrimary()
}

func (p *Parser) sign() (bool, error) {
	if _, err := p.accept(plus); err == nil {
		return false, nil
	}
	if _, err := p.accept(minus); err == nil {
		return true, nil
	}
	return false, xerrors.New("neither plus nor minus")
}

func (p *Parser) numericPrimary() (interface{}, error) {
	return p.valueExpressionPrimary()
}

func (p *Parser) valueExpressionPrimary() (interface{}, error) {
	return p.unparenthesizedValueExpressionPrimary()
}

func (p *Parser) unparenthesizedValueExpressionPrimary() (interface{}, error) {
	return p.unsignedLiteral()
}

func (p *Parser) unsignedLiteral() (interface{}, error) {
	if v, err := p.unsignedNumericLiteral(); err == nil {
		return v, nil
	}
	if v, err := p.generalLiteral(); err == nil {
		return v, nil
	}
	return nil, xerrors.New("non unsigned literal")
}

func (p *Parser) unsignedNumericLiteral() (interface{}, error) {
	return p.exactNumericLiteral()
}

func (p *Parser) exactNumericLiteral() (interface{}, error) {
	v, err := p.accept(unsignedNumeric)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (p *Parser) generalLiteral() (interface{}, error) {
	return p.characterStringLiteral()
}

func (p *Parser) characterStringLiteral() (interface{}, error) {
	return p.accept(characterString)
}

func (p *Parser) stringValueExpression() (string, error) {
	return p.characterValueExpression()
}

func (p *Parser) characterValueExpression() (string, error) {
	if v, err := p.characterFactor(); err == nil {
		return v, nil
	}
	return "", xerrors.New("non character value expression")
}

func (p *Parser) characterFactor() (string, error) {
	return p.characterPrimary()
}

func (p *Parser) characterPrimary() (string, error) {
	if v, err := p.valueExpressionPrimary(); err == nil {
		s, ok := v.(string)
		if !ok {
			return "", xerrors.New("non string value")
		}
		return s, nil
	}
	return "", xerrors.New("non character primary")
}

func (p *Parser) fromDefault() (Source, error) {
	if _, err := p.accept(kwDefault); err != nil {
		return nil, err
	}
	if _, err := p.accept(kwValues); err != nil {
		return nil, err
	}
	return nil, nil
}

func (p *Parser) sqlSchemaStatement() (driver.Stmt, error) {
	return p.sqlSchemaDefinitionStatement()
}

func (p *Parser) sqlSchemaDefinitionStatement() (driver.Stmt, error) {
	return p.TableDefinition()
}

func (p *Parser) TableDefinition() (*TableDefinition, error) {
	var t TableDefinition
	start := p.token.start
	defer func() {
		end := p.token.end
		t.RawSQL = p.lex.input[start:end]
	}()

	if _, err := p.accept(kwCreate); err != nil {
		return nil, err
	}
	if _, err := p.accept(kwTable); err != nil {
		return nil, err
	}
	val, err := p.accept(identifier)
	if err != nil {
		return nil, err
	}
	t.Name = val.(string)

	if err := p.tableContentsSource(&t); err != nil {
		return nil, xerrors.Errorf("while parsing table contents source: %w", err)
	}

	return &t, nil
}

func (p *Parser) tableContentsSource(t *TableDefinition) error {
	if err := p.tableElementsList(t); err != nil {
		return xerrors.Errorf("while parsing table elements list: %w", err)
	}
	return nil
}

func (p *Parser) tableElementsList(t *TableDefinition) error {
	if _, err := p.accept(leftParen); err != nil {
		return err
	}
	if err := p.tableElement(t); err != nil {
		return err
	}
	for p.token.typ == comma {
		if _, err := p.accept(comma); err != nil {
			return err
		}
		if err := p.tableElement(t); err != nil {
			return xerrors.Errorf("while parsing table element: %w", err)
		}
	}
	if _, err := p.accept(rightParen); err != nil {
		return err
	}
	return nil
}

func (p *Parser) tableElement(t *TableDefinition) error {
	col, err := p.columnDefinition()
	switch {
	case xerrors.Is(err, ErrIncomplete):
		return err
	case err != nil:
		err := p.tableConstraintDefinition(t)
		switch {
		case xerrors.Is(err, ErrIncomplete):
			return err
		case err != nil:
			return xerrors.New("neither column definition nor table constraint definition")
		default:
			return nil
		}
	default:
		t.Columns = append(t.Columns, *col)
		return nil
	}
}

func (p *Parser) columnDefinition() (*ColumnDefinition, error) {
	var col ColumnDefinition
	val, err := p.accept(identifier)
	if err != nil {
		return nil, err
	}
	col.Name = val.(string)
	col.DataType, err = p.dataType()
	if err != nil {
		return nil, xerrors.Errorf("while parsing data type: %w", err)
	}
	return &col, nil
}

func (p *Parser) dataType() (DataType, error) {
	if _, err := p.accept(kwText); err == nil {
		return Text, nil
	}
	if _, err := p.accept(kwInteger); err == nil {
		return Integer, nil
	}
	return -1, xerrors.New("unknown data type")
}

func (p *Parser) tableConstraintDefinition(t *TableDefinition) error {
	if err := p.primaryKeyConstraintDefinition(t); err != nil {
		return xerrors.Errorf("while parsing primary key constraint definition: %w", err)
	}
	return nil
}

func (p *Parser) primaryKeyConstraintDefinition(t *TableDefinition) error {
	if _, err := p.accept(kwPrimary); err != nil {
		return err
	}
	if _, err := p.accept(kwKey); err != nil {
		return err
	}
	if _, err := p.accept(leftParen); err != nil {
		return err
	}
	cols, err := p.columnNameList()
	if err != nil {
		return err
	}
	if _, err := p.accept(rightParen); err != nil {
		return err
	}
	t.PrimaryKey = cols
	return nil
}

func (p *Parser) columnNameList() ([]string, error) {
	var cols []string
	val, err := p.accept(identifier)
	if err != nil {
		return nil, err
	}
	cols = append(cols, val.(string))
	for p.token.typ == comma {
		if _, err := p.accept(comma); err != nil {
			return nil, err
		}
		val, err := p.accept(identifier)
		if err != nil {
			return nil, err
		}
		cols = append(cols, val.(string))
	}
	return cols, nil
}

type Source interface {
	Columns() []ColumnDefinition
	Next([]interface{}) bool
}

type FromConstructorSource [][]interface{}

func (f *FromConstructorSource) Columns() []ColumnDefinition {
	cds := make([]ColumnDefinition, len((*f)[0]))
	for i, c := range (*f)[0] {
		switch c.(type) {
		case string:
			cds[i] = ColumnDefinition{
				DataType: Text,
			}
		case int64:
			cds[i] = ColumnDefinition{
				DataType: Integer,
			}
		}
	}
	return cds
}

func (f *FromConstructorSource) Next(val []interface{}) bool {
	if len(*f) == 0 {
		return false
	}
	copy(val, (*f)[0])
	*f = (*f)[1:]
	return true
}
