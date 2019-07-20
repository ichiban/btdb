package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"golang.org/x/crypto/ssh/terminal"
	"golang.org/x/xerrors"

	"github.com/ichiban/btdb"
	"github.com/ichiban/btdb/sql"
	"github.com/ichiban/linesqueak"
)

func main() {
	filename := os.Args[1]
	db, err := btdb.Open(filename)
	if err != nil {
		db, err = btdb.Create(filename)
		if err != nil {
			log.Fatalf("failed to open file: %v", err)
		}
	}
	defer func() {
		_ = db.Close()
	}()

	oldState, err := terminal.MakeRaw(0)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := terminal.Restore(0, oldState); err != nil {
			log.Fatalf("terminal.Restore() failed: %v", err)
		}
	}()

	base := filepath.Base(os.Args[0])
	prompt := fmt.Sprintf("%s> ", base)
	contPrompt := fmt.Sprintf("%s> ", dots(base))

	e := &linesqueak.Editor{
		In:     bufio.NewReader(os.Stdin),
		Out:    bufio.NewWriter(os.Stdout),
		Prompt: prompt,
	}

	var s []string
	for {
		l, err := e.Line()
		if err != nil {
			_, _ = fmt.Fprintf(e, "error: %v\n", err)
			break
		}
		_, _ = fmt.Fprintf(e, "%s%s\n", e.Prompt, l)
		e.History.Add(l)
		s = append(s, l)
		_, err = db.QueryContext(context.Background(), strings.Join(s, "\n"), nil)
		switch {
		case xerrors.Is(err, sql.ErrIncomplete):
			e.Prompt = contPrompt
		case err != nil:
			_, _ = fmt.Fprintf(e, "error: %+v\n", err)
			s = s[:0]
			e.Prompt = prompt
		default:
			s = s[:0]
			e.Prompt = prompt
		}
	}
}

func dots(s string) string {
	ret := make([]rune, utf8.RuneCountInString(s))
	for i := range s {
		ret[i] = '.'
	}
	return string(ret)
}
