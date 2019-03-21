package sql

import (
	"testing"

	"github.com/ichiban/btdb/sql/ast"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser_DirectSQLStatement(t *testing.T) {
	t.Run("create table dept", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)
		p := NewParser(`
create table dept(  
  deptno     integer,
  dname      text,
  loc        text,
  primary key (deptno)  
);
`)
		d, err := p.DirectSQLStatement()
		assert.NoError(err)
		require.IsType(&ast.TableDefinition{}, d)
		td := d.(*ast.TableDefinition)
		assert.Nil(td.Scope)
		assert.Equal("dept", td.Name)
		assert.Len(td.Columns, 3)
		assert.Equal("deptno", td.Columns[0].Name)
		assert.Equal(ast.Integer, td.Columns[0].DataType)
		assert.Equal("dname", td.Columns[1].Name)
		assert.Equal(ast.Text, td.Columns[1].DataType)
		assert.Equal("loc", td.Columns[2].Name)
		assert.Equal(ast.Text, td.Columns[2].DataType)
		assert.Equal([]string{"deptno"}, td.PrimaryKey)
	})
}
