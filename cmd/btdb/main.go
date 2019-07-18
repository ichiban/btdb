package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ichiban/btdb"

	"golang.org/x/crypto/ssh/terminal"

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
		_, err = db.QueryContext(context.Background(), l, nil)
		if err != nil {
			_, _ = fmt.Fprintf(e, "error: %v\n", err)
		}
	}
}
