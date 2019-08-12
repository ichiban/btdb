package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser_DirectSQLStatement(t *testing.T) {
	t.Run("create table dept", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)
		p := NewParser(nil, `
create table dept(  
  deptno     integer,
  dname      text,
  loc        text,
  primary key (deptno)  
);
`)
		s, err := p.DirectSQLStatement()
		assert.NoError(err)
		require.IsType(&TableDefinition{}, s)
		td := s.(*TableDefinition)
		assert.Equal(`create table dept(  
  deptno     integer,
  dname      text,
  loc        text,
  primary key (deptno)  
);`, td.RawSQL)
		assert.Equal("dept", td.Name)
		assert.Len(td.Columns, 3)
		assert.Equal("deptno", td.Columns[0].Name)
		assert.Equal(Integer, td.Columns[0].DataType)
		assert.Equal("dname", td.Columns[1].Name)
		assert.Equal(Text, td.Columns[1].DataType)
		assert.Equal("loc", td.Columns[2].Name)
		assert.Equal(Text, td.Columns[2].DataType)
		assert.Equal([]string{"deptno"}, td.PrimaryKey)
	})

	t.Run("insert into dept", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)
		p := NewParser(nil, `
insert into DEPT (DEPTNO, DNAME, LOC)
values(10, 'ACCOUNTING', 'NEW YORK');
`)
		s, err := p.DirectSQLStatement()
		assert.NoError(err)
		require.IsType(&InsertStatement{}, s)
		is := s.(*InsertStatement)
		assert.Equal("DEPT", is.Target)
		vs := make([]interface{}, len(is.Source.Columns()))
		assert.True(is.Source.Next(vs))
		assert.Equal([]interface{}{int64(10), "ACCOUNTING", "NEW YORK"}, vs)
		assert.False(is.Source.Next(vs))
	})
}
