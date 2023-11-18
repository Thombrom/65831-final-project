package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
)

type LogSequenceNumber *int

var nextLSN = 0

func NewLSN() LogSequenceNumber {
	id := nextLSN
	nextLSN++
	return &id
}

// An append only log that force writes
// to disk
type Log struct {
	file *os.File
}

type PositionDescriptor struct {
	PageNo int64
	SlotNo int64
}

type OperationType int

const (
	OperationWrite  OperationType = iota
	OperationDelete OperationType = iota
)

type LogOperation struct {
	// The tuple the operation
	tuple     *Tuple
	pd        PositionDescriptor
	operation OperationType
}

// Log records are serialized with the size of the
// log record as the first bytes, then the string
// encoded with the filename length as the first
// value followed by those bytes to allow for variable
// length filenames, and knowing which records are
// for which heapfiles
type LogRecord struct {
	filename string
	lsn      LogSequenceNumber
	tid      TransactionID
	prev_lsn LogSequenceNumber

	// Actually redo/undo operations
	undo *LogOperation
	redo *LogOperation
}

// Assumes that recovery has been performed before creating this new
// log.
func newLog(fromFile string) (*Log, error) {
	file, err := os.OpenFile(fromFile, os.O_RDWR|os.O_CREATE, fs.ModeAppend)
	if err != nil {
		return nil, err
	}

	return &Log{file}, nil
}

func (t *LogRecord) writeTo(b *bytes.Buffer) error {
	sub_buf := new(bytes.Buffer)

	err := errors.Join(
		// Record filename
		binary.Write(sub_buf, binary.LittleEndian, int64(len(t.filename))),
		binary.Write(sub_buf, binary.LittleEndian, []byte(t.filename)),

		// Record next items
		binary.Write(sub_buf, binary.LittleEndian, int64(*t.lsn)),
		binary.Write(sub_buf, binary.LittleEndian, int64(*t.tid)),
		binary.Write(sub_buf, binary.LittleEndian, int64(*t.prev_lsn)),

		// Record undo and redo
		t.undo.writeTo(sub_buf),
		t.redo.writeTo(sub_buf),
	)

	if err != nil {
		return err
	}

	// Record the size as the first value followed by the bytes
	err = binary.Write(b, binary.LittleEndian, int64(len(sub_buf.Bytes())))
	if err != nil {
		return err
	}

	_, err = b.Write((sub_buf.Bytes()))
	return err
}

func (t *LogRecord) equals(other *LogRecord) bool {
	return t.filename == other.filename &&
		*t.lsn == *other.lsn &&
		*t.prev_lsn == *other.prev_lsn &&
		*t.tid == *other.tid &&
		t.redo.equals(other.redo) &&
		t.undo.equals(other.undo)
}

func (t *LogOperation) equals(other *LogOperation) bool {
	return t.tuple.equals(other.tuple) &&
		t.pd == other.pd &&
		t.operation == other.operation
}

func (t *LogOperation) writeTo(b *bytes.Buffer) error {
	return errors.Join(
		binary.Write(b, binary.LittleEndian, int64(t.operation)),
		binary.Write(b, binary.LittleEndian, t.pd),
		t.tuple.writeTo(b),
	)
}

func read_int_log(b *bytes.Buffer) (int, error) {
	value := int64(0)
	err := binary.Read(b, binary.LittleEndian, &value)
	return int(value), err
}

func read_string_log(b *bytes.Buffer, length int) (string, error) {
	value := make([]byte, length)
	err := binary.Read(b, binary.LittleEndian, &value)
	return string(value), err
}

// Assumes bytes points to the start of a serialization
func ReadFilenameFrom(b *bytes.Buffer) (string, error) {
	// Length is always first
	length, err := read_int_log(b)
	if err != nil {
		return "", err
	}

	if length <= 4 {
		return "", fmt.Errorf("Not long enough")
	}

	filename_length, err := read_int_log(b)
	if err != nil {
		return "", err
	}

	if length < filename_length+4 {
		return "", fmt.Errorf("Filename exceeds log record")
	}

	return read_string_log(b, int(filename_length))
}

func ReadLogOperationFrom(b *bytes.Buffer, desc *TupleDesc) (*LogOperation, error) {
	operation, err := read_int_log(b)
	if err != nil {
		return nil, err
	}

	pd := &PositionDescriptor{0, 0}
	err = binary.Read(b, binary.LittleEndian, pd)
	if err != nil {
		return nil, err
	}

	tuple, err := readTupleFrom(b, desc)
	if err != nil {
		return nil, err
	}

	return &LogOperation{tuple, *pd, OperationType(operation)}, nil
}

func ReadLogRecordFrom(b *bytes.Buffer, desc *TupleDesc) (*LogRecord, error) {
	filename, err := ReadFilenameFrom(b)
	if err != nil {
		return nil, err
	}

	lsn, err1 := read_int_log(b)
	tid, err2 := read_int_log(b)
	prev_lsn, err3 := read_int_log(b)

	err = errors.Join(err1, err2, err3)
	if err != nil {
		return nil, err
	}

	undo, err := ReadLogOperationFrom(b, desc)
	if err != nil {
		return nil, err
	}

	redo, err := ReadLogOperationFrom(b, desc)
	if err != nil {
		return nil, err
	}

	return &LogRecord{filename, &lsn, &tid, &prev_lsn, undo, redo}, err
}
