package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/ichiban/btdb/sql"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/ichiban/linesqueak"
)

func main() {
	oldState, err := terminal.MakeRaw(0)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := terminal.Restore(0, oldState); err != nil {
			log.Fatalf("terminal.Restore() failed: %v", err)
		}
	}()

	e := &linesqueak.Editor{
		In:     bufio.NewReader(os.Stdin),
		Out:    bufio.NewWriter(os.Stdout),
		Prompt: "> ",
	}

	for {
		l, err := e.Line()
		if err != nil {
			_, _ = fmt.Fprintf(e, "error: %v\n", err)
			break
		}
		_, _ = fmt.Fprintf(e, "%s%s\n", e.Prompt, l)
		e.History.Add(l)
		p := sql.NewParser(l)
		_, err = p.DirectSQLStatement()
		if err != nil {
			_, _ = fmt.Fprintf(e, "syntax error: %v\n", err)
		}
	}
}
