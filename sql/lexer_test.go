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

		assert.Equal(token{typ: characterString, val: "foo"}, l.Next())
	})

	t.Run("simple select", func(t *testing.T) {
		assert := assert.New(t)

		l := NewLexer("SELECT * FROM Customers;")
		go l.Run()

		assert.Equal(token{typ: kwSelect}, l.Next())
		assert.Equal(token{typ: asterisk}, l.Next())
		assert.Equal(token{typ: kwFrom}, l.Next())
		assert.Equal(token{typ: identifier, val: "Customers"}, l.Next())
		assert.Equal(token{typ: semicolon}, l.Next())
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

		assert.Equal(token{typ: kwCreate}, l.Next())
		assert.Equal(token{typ: kwTable}, l.Next())
		assert.Equal(token{typ: identifier, val: "dept"}, l.Next())
		assert.Equal(token{typ: leftParen}, l.Next())
		assert.Equal(token{typ: identifier, val: "deptno"}, l.Next())
		assert.Equal(token{typ: kwInteger}, l.Next())
		assert.Equal(token{typ: kwPrimary}, l.Next())
		assert.Equal(token{typ: kwKey}, l.Next())
		assert.Equal(token{typ: comma}, l.Next())
		assert.Equal(token{typ: identifier, val: "dname"}, l.Next())
		assert.Equal(token{typ: kwText}, l.Next())
		assert.Equal(token{typ: comma}, l.Next())
		assert.Equal(token{typ: identifier, val: "loc"}, l.Next())
		assert.Equal(token{typ: kwText}, l.Next())
		assert.Equal(token{typ: comma}, l.Next())
		assert.Equal(token{typ: rightParen}, l.Next())
	})

	t.Run("simple insert", func(t *testing.T) {
		assert := assert.New(t)

		l := NewLexer(`
insert into DEPT (DEPTNO, DNAME, LOC)
values(10, 'ACCOUNTING', 'NEW YORK');
`)
		go l.Run()

		assert.Equal(token{typ: kwInsert}, l.Next())
		assert.Equal(token{typ: kwInto}, l.Next())
		assert.Equal(token{typ: identifier, val: "DEPT"}, l.Next())
		assert.Equal(token{typ: leftParen}, l.Next())
		assert.Equal(token{typ: identifier, val: "DEPTNO"}, l.Next())
		assert.Equal(token{typ: comma}, l.Next())
		assert.Equal(token{typ: identifier, val: "DNAME"}, l.Next())
		assert.Equal(token{typ: comma}, l.Next())
		assert.Equal(token{typ: identifier, val: "LOC"}, l.Next())
		assert.Equal(token{typ: rightParen}, l.Next())
		assert.Equal(token{typ: kwValues}, l.Next())
		assert.Equal(token{typ: leftParen}, l.Next())
		assert.Equal(token{typ: unsignedNumeric, val: int64(10)}, l.Next())
		assert.Equal(token{typ: comma}, l.Next())
		assert.Equal(token{typ: characterString, val: "ACCOUNTING"}, l.Next())
		assert.Equal(token{typ: comma}, l.Next())
		assert.Equal(token{typ: characterString, val: "NEW YORK"}, l.Next())
		assert.Equal(token{typ: rightParen}, l.Next())
		assert.Equal(token{typ: semicolon}, l.Next())
	})
}
