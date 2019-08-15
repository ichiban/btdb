package sql

import (
	"database/sql/driver"
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
insert into dept (deptno, dname, loc)
values
(10, 'ACCOUNTING', 'NEW YORK'),
(20, 'MARKETING', 'SAN FRANCISCO'),
(30, 'HR', 'TOKYO');
`)
		s, err := p.DirectSQLStatement()
		assert.NoError(err)
		require.IsType(&InsertStatement{}, s)
		is := s.(*InsertStatement)
		assert.Equal("dept", is.Target)
		assert.Equal([]string{"deptno", "dname", "loc"}, is.Source.Columns())
		vs := make([]driver.Value, len(is.Source.Columns()))
		assert.NoError(is.Source.Next(vs))
		assert.Equal([]driver.Value{int64(10), "ACCOUNTING", "NEW YORK"}, vs)
		assert.NoError(is.Source.Next(vs))
		assert.Equal([]driver.Value{int64(20), "MARKETING", "SAN FRANCISCO"}, vs)
		assert.NoError(is.Source.Next(vs))
		assert.Equal([]driver.Value{int64(30), "HR", "TOKYO"}, vs)
		assert.Error(is.Source.Next(vs))
	})

	t.Run("simple select", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)
		p := NewParser(nil, `
SELECT * FROM dept;
`)
		s, err := p.DirectSQLStatement()
		assert.NoError(err)
		require.IsType(&SelectStatement{}, s)
		ss := s.(*SelectStatement)
		assert.Equal("dept", ss.From)
	})
}
