package btdb

import (
	"encoding/binary"
	"errors"
	"io"
)

// TODO: fix signature
var signature = []byte{0x37, 0x7f, 0x06, 0x81}

var ErrInvalidDatabaseFile = errors.New("invalid database file")

type Database struct {
	io.ReadWriteSeeker
	PageSize uint32
}

func NewDatabase(rws io.ReadWriteSeeker) (*Database, error) {
	head, _ := rws.Seek(0, io.SeekCurrent)
	defer rws.Seek(head, io.SeekStart)

	if err := checkSignature(rws); err != nil {
		return nil, err
	}

	var db Database
	if err := binary.Read(rws, binary.BigEndian, db.PageSize); err != nil {
		return nil, err
	}

	return &db, nil
}

func checkSignature(r io.Reader) error {
	var sig [4]byte
	if err := binary.Read(r, binary.BigEndian, &sig); err != nil {
		return err
	}
	for i, b := range sig {
		if b != signature[i] {
			return ErrInvalidDatabaseFile
		}
	}
	return nil
}
