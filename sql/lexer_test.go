package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexer_Next(t *testing.T) {
	t.Run("character string literal", func(t *testing.T) {
		assert := assert.New(t)

		l := NewLexer(`'foo'`)
		go l.Run()

		assert.Equal(token{start: 0, end: 5, typ: characterString, val: "foo"}, l.Next())
	})

	t.Run("simple select", func(t *testing.T) {
		assert := assert.New(t)

		l := NewLexer("SELECT * FROM Customers;")
		go l.Run()

		assert.Equal(token{start: 0, end: 6, typ: kwSelect}, l.Next())
		assert.Equal(token{start: 7, end: 8, typ: asterisk}, l.Next())
		assert.Equal(token{start: 9, end: 13, typ: kwFrom}, l.Next())
		assert.Equal(token{start: 14, end: 23, typ: identifier, val: "Customers"}, l.Next())
		assert.Equal(token{start: 23, end: 24, typ: semicolon}, l.Next())
	})

	t.Run("create table dept", func(t *testing.T) {
		assert := assert.New(t)

		l := NewLexer(`
create table dept(  
  deptno     integer primary key,
  dname      text,
  loc        text,
)
`)
		go l.Run()

		assert.Equal(token{start: 1, end: 7, typ: kwCreate}, l.Next())
		assert.Equal(token{start: 8, end: 13, typ: kwTable}, l.Next())
		assert.Equal(token{start: 14, end: 18, typ: identifier, val: "dept"}, l.Next())
		assert.Equal(token{start: 18, end: 19, typ: leftParen}, l.Next())
		assert.Equal(token{start: 24, end: 30, typ: identifier, val: "deptno"}, l.Next())
		assert.Equal(token{start: 35, end: 42, typ: kwInteger}, l.Next())
		assert.Equal(token{start: 43, end: 50, typ: kwPrimary}, l.Next())
		assert.Equal(token{start: 51, end: 54, typ: kwKey}, l.Next())
		assert.Equal(token{start: 54, end: 55, typ: comma}, l.Next())
		assert.Equal(token{start: 58, end: 63, typ: identifier, val: "dname"}, l.Next())
		assert.Equal(token{start: 69, end: 73, typ: kwText}, l.Next())
		assert.Equal(token{start: 73, end: 74, typ: comma}, l.Next())
		assert.Equal(token{start: 77, end: 80, typ: identifier, val: "loc"}, l.Next())
		assert.Equal(token{start: 88, end: 92, typ: kwText}, l.Next())
		assert.Equal(token{start: 92, end: 93, typ: comma}, l.Next())
		assert.Equal(token{start: 94, end: 95, typ: rightParen}, l.Next())
	})

	t.Run("simple insert", func(t *testing.T) {
		assert := assert.New(t)

		l := NewLexer(`
insert into DEPT (DEPTNO, DNAME, LOC)
values(10, 'ACCOUNTING', 'NEW YORK');
`)
		go l.Run()

		assert.Equal(token{start: 1, end: 7, typ: kwInsert}, l.Next())
		assert.Equal(token{start: 8, end: 12, typ: kwInto}, l.Next())
		assert.Equal(token{start: 13, end: 17, typ: identifier, val: "DEPT"}, l.Next())
		assert.Equal(token{start: 18, end: 19, typ: leftParen}, l.Next())
		assert.Equal(token{start: 19, end: 25, typ: identifier, val: "DEPTNO"}, l.Next())
		assert.Equal(token{start: 25, end: 26, typ: comma}, l.Next())
		assert.Equal(token{start: 27, end: 32, typ: identifier, val: "DNAME"}, l.Next())
		assert.Equal(token{start: 32, end: 33, typ: comma}, l.Next())
		assert.Equal(token{start: 34, end: 37, typ: identifier, val: "LOC"}, l.Next())
		assert.Equal(token{start: 37, end: 38, typ: rightParen}, l.Next())
		assert.Equal(token{start: 39, end: 45, typ: kwValues}, l.Next())
		assert.Equal(token{start: 45, end: 46, typ: leftParen}, l.Next())
		assert.Equal(token{start: 46, end: 48, typ: unsignedNumeric, val: int64(10)}, l.Next())
		assert.Equal(token{start: 48, end: 49, typ: comma}, l.Next())
		assert.Equal(token{start: 50, end: 62, typ: characterString, val: "ACCOUNTING"}, l.Next())
		assert.Equal(token{start: 62, end: 63, typ: comma}, l.Next())
		assert.Equal(token{start: 64, end: 74, typ: characterString, val: "NEW YORK"}, l.Next())
		assert.Equal(token{start: 74, end: 75, typ: rightParen}, l.Next())
		assert.Equal(token{start: 75, end: 76, typ: semicolon}, l.Next())
	})
}
