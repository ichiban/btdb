package sql

import (
	"errors"

	"golang.org/x/xerrors"

	"github.com/ichiban/btdb/sql/ast"
)

type Parser struct {
	lex   *Lexer
	token token
}

func NewParser(input string) *Parser {
	l := NewLexer(input)
	go l.Run()
	return &Parser{
		lex:   l,
		token: l.Next(),
	}
}

func (p *Parser) accept(typ tokenType) (interface{}, error) {
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

func (p *Parser) DirectSQLStatement() (interface{}, error) {
	stmt, err := p.directlyExecutableStatement()
	if err != nil {
		return nil, xerrors.Errorf("while parsing directly executable statement: %w", err)
	}
	if _, err := p.accept(semicolon); err != nil {
		return nil, err
	}
	return stmt, nil
}

func (p *Parser) directlyExecutableStatement() (interface{}, error) {
	/*
		stmt, err := p.directSQLDataStatement()
		if err != nil {
			return nil, err
		}
	*/
	stmt, err := p.sqlSchemaStatement()
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

func (p *Parser) directSQLDataStatement() (interface{}, error) {
	return nil, xerrors.New("not implemented")
}

func (p *Parser) sqlSchemaStatement() (interface{}, error) {
	return p.sqlSchemaDefinitionStatement()
}

func (p *Parser) sqlSchemaDefinitionStatement() (interface{}, error) {
	return p.tableDefinition()
}

func (p *Parser) tableDefinition() (*ast.TableDefinition, error) {
	var t ast.TableDefinition
	if _, err := p.accept(kwCreate); err != nil {
		return nil, err
	}
	scope, _ := p.tableScope()
	t.Scope = scope
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

func (p *Parser) tableScope() (*ast.TableScope, error) {
	scope, err := p.globalOrLocal()
	if err != nil {
		return nil, err
	}
	if _, err := p.accept(kwTemporary); err != nil {
		return nil, err
	}
	return &scope, nil
}

func (p *Parser) globalOrLocal() (ast.TableScope, error) {
	if _, err := p.accept(kwGlobal); err != nil {
		return ast.GlobalTableScope, nil
	}
	if _, err := p.accept(kwLocal); err != nil {
		return ast.LocalTableScope, nil
	}
	return -1, errors.New("unknown table scope")
}

func (p *Parser) tableContentsSource(t *ast.TableDefinition) error {
	if err := p.tableElementsList(t); err != nil {
		return xerrors.Errorf("while parsing table elements list: %w", err)
	}
	return nil
}

func (p *Parser) tableElementsList(t *ast.TableDefinition) error {
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

func (p *Parser) tableElement(t *ast.TableDefinition) error {
	if col, err := p.columnDefinition(); err == nil {
		t.Columns = append(t.Columns, *col)
		return nil
	}
	if err := p.tableConstraintDefinition(t); err == nil {
		return nil
	}

	return xerrors.New("neither column definition nor table constraint definition")
}

func (p *Parser) columnDefinition() (*ast.ColumnDefinition, error) {
	var col ast.ColumnDefinition
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

func (p *Parser) dataType() (ast.DataType, error) {
	if _, err := p.accept(kwText); err == nil {
		return ast.Text, nil
	}
	if _, err := p.accept(kwInteger); err == nil {
		return ast.Integer, nil
	}
	return -1, xerrors.New("unknown data type")
}

func (p *Parser) tableConstraintDefinition(t *ast.TableDefinition) error {
	if err := p.primaryKeyConstraintDefinition(t); err != nil {
		return xerrors.Errorf("while parsing primary key constraint definition: %w", err)
	}
	return nil
}

func (p *Parser) primaryKeyConstraintDefinition(t *ast.TableDefinition) error {
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
