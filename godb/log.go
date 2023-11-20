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

func file_exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

type OperationType int

const (
	LogBeginTransaction  = iota
	LogAbortTransaction  = iota
	LogCommitTransaction = iota
	LogInsertDelete      = iota
)

// An append only log that force writes
// to disk
type Log struct {
	fromfile string

	// The file is the log file, and it is opened
	// in append mode to have atomic appends on UNIX
	file *os.File

	// Maps the most recent log record written by that
	// transaction
	transaction_table map[TransactionID]LogSequenceNumber

	// Maps each pageno to the log record that first
	// dirtied that page
	dirty_page_table map[heapHash]LogSequenceNumber

	// The offset into the log file for the last checkpoint
	// This value is used for faster recovery
	checkpoint_offset int64
}

type PositionDescriptor struct {
	PageNo int64
	SlotNo int64
}

type LogOperation struct {
	// The tuple the operation
	has_tuple bool
	tuple     *Tuple
	pd        PositionDescriptor
}

// Log records are serialized with the size of the
// log record as the first bytes, then the string
// encoded with the filename length as the first
// value followed by those bytes to allow for variable
// length filenames, and knowing which records are
// for which heapfiles
type LogRecord struct {
	// Filename is an empty string when optype != InsertDelete
	filename string
	lsn      LogSequenceNumber
	tid      TransactionID
	prev_lsn LogSequenceNumber
	optype   OperationType

	// Actually redo/undo operations
	undo *LogOperation
	redo *LogOperation
}

func log_filename(fromfile string) string {
	return fromfile + ".log"
}

func dirty_page_table_filename(fromfile string) string {
	return fromfile + ".dirty"
}

func transaction_table_filename(fromfile string) string {
	return fromfile + ".txn"
}

func checkpoint_filename(fromfile string) string {
	return fromfile + ".chkpnt"
}

func recover_dirty_page_table(fromfile string) map[heapHash]LogSequenceNumber {
	filename := dirty_page_table_filename(fromfile)
	mapping := make(map[heapHash]LogSequenceNumber)

	if file_exists(filename) {
		file, _ := os.Open(filename)
		defer file.Close()

		stat, _ := file.Stat()
		buf := make([]byte, stat.Size())
		file.Read(buf)

		bbuf := bytes.NewBuffer(buf)
		binary.Read(bbuf, binary.LittleEndian, mapping)
	}

	return mapping
}

func recover_transaction_table(fromfile string) map[TransactionID]LogSequenceNumber {
	filename := transaction_table_filename(fromfile)
	mapping := make(map[TransactionID]LogSequenceNumber)

	if file_exists(filename) {
		file, _ := os.Open(filename)
		defer file.Close()

		stat, _ := file.Stat()
		buf := make([]byte, stat.Size())
		file.Read(buf)

		bbuf := bytes.NewBuffer(buf)
		binary.Read(bbuf, binary.LittleEndian, mapping)
	}

	return mapping
}

func recover_checkpoint(fromfile string) int64 {
	filename := checkpoint_filename(fromfile)
	checkpoint := int64(0)

	if file_exists(filename) {
		file, _ := os.Open(filename)
		defer file.Close()

		stat, _ := file.Stat()
		buf := make([]byte, stat.Size())
		file.Read(buf)

		bbuf := bytes.NewBuffer(buf)
		binary.Read(bbuf, binary.LittleEndian, &checkpoint)
	}

	return checkpoint
}

// FromFile is a family of files. The .log is the file containing the log. The
// .dirty is the dirty page table file, the .txn is the transactions table and
// the .chkpnt is the checkpoint table
func newLog(fromfile string) (*Log, error) {
	file, err := os.OpenFile(fromfile, os.O_RDWR|os.O_CREATE, fs.ModeAppend)
	if err != nil {
		return nil, err
	}

	dirty_page_table := recover_dirty_page_table(fromfile)
	transaction_table := recover_transaction_table(fromfile)
	checkpoint := recover_checkpoint(fromfile)

	return &Log{fromfile, file, transaction_table, dirty_page_table, checkpoint}, nil
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
		binary.Write(sub_buf, binary.LittleEndian, int64(t.optype)),
	)

	// Record undo and redo
	if t.optype == LogInsertDelete {
		err = errors.Join(
			err,
			t.undo.writeTo(sub_buf),
			t.redo.writeTo(sub_buf),
		)
	}

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
	eq := t.filename == other.filename &&
		*t.lsn == *other.lsn &&
		*t.prev_lsn == *other.prev_lsn &&
		*t.tid == *other.tid &&
		t.optype == other.optype

	if t.optype == LogInsertDelete {
		eq = eq &&
			t.redo.equals(other.redo) &&
			t.undo.equals(other.undo)
	}

	return eq
}

func (t *LogOperation) equals(other *LogOperation) bool {
	eq := t.has_tuple == other.has_tuple && t.pd == other.pd
	if t.has_tuple {
		eq = eq && t.tuple.equals(other.tuple)
	}

	return eq
}

func (t *LogOperation) writeTo(b *bytes.Buffer) error {
	err := binary.Write(b, binary.LittleEndian, t.pd)
	if err != nil {
		return err
	}

	err = binary.Write(b, binary.LittleEndian, t.has_tuple)
	if err != nil {
		return err
	}

	if t.has_tuple {
		return t.tuple.writeTo(b)
	}
	return nil
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
	pd := &PositionDescriptor{0, 0}
	err := binary.Read(b, binary.LittleEndian, pd)
	if err != nil {
		return nil, err
	}

	var has_tuple bool
	err = binary.Read(b, binary.LittleEndian, &has_tuple)
	if err != nil {
		return nil, err
	}

	if has_tuple {
		tuple, err := readTupleFrom(b, desc)
		if err != nil {
			return nil, err
		}

		return &LogOperation{has_tuple, tuple, *pd}, nil
	}

	return &LogOperation{has_tuple, nil, *pd}, nil
}

func ReadLogRecordFrom(b *bytes.Buffer, desc *TupleDesc) (*LogRecord, error) {
	filename, err := ReadFilenameFrom(b)
	if err != nil {
		return nil, err
	}

	lsn, err1 := read_int_log(b)
	tid, err2 := read_int_log(b)
	prev_lsn, err3 := read_int_log(b)
	optype, err4 := read_int_log(b)
	optype_typed := OperationType(optype)

	err = errors.Join(err1, err2, err3, err4)
	if err != nil {
		return nil, err
	}

	if optype_typed != LogInsertDelete {
		return &LogRecord{filename, &lsn, &tid, &prev_lsn, optype_typed, nil, nil}, nil
	}

	undo, err := ReadLogOperationFrom(b, desc)
	if err != nil {
		return nil, err
	}

	redo, err := ReadLogOperationFrom(b, desc)
	if err != nil {
		return nil, err
	}

	return &LogRecord{filename, &lsn, &tid, &prev_lsn, optype_typed, undo, redo}, err
}

// TODO: Mutex
func (t *Log) InsertLog(filename string, tid TransactionID, position PositionDescriptor, new_tuple *Tuple) *LogRecord {
	lsn := NewLSN()

	heap_hash := heapHash{FileName: filename, PageNo: int(position.PageNo)}
	_, ok := t.dirty_page_table[heap_hash]
	if !ok {
		t.dirty_page_table[heap_hash] = lsn
	}

	prev_lsn := t.transaction_table[tid]
	t.transaction_table[tid] = lsn

	return &LogRecord{filename, lsn, tid, prev_lsn, LogInsertDelete, &LogOperation{false, nil, position}, &LogOperation{true, new_tuple, position}}
}

func (t *Log) DeleteLog(filename string, tid TransactionID, position PositionDescriptor, prev_tuple *Tuple) *LogRecord {
	lsn := NewLSN()

	heap_hash := heapHash{FileName: filename, PageNo: int(position.PageNo)}
	_, ok := t.dirty_page_table[heap_hash]
	if !ok {
		t.dirty_page_table[heap_hash] = lsn
	}

	prev_lsn := t.transaction_table[tid]
	t.transaction_table[tid] = lsn

	return &LogRecord{filename, lsn, tid, prev_lsn, LogInsertDelete, &LogOperation{true, prev_tuple, position}, &LogOperation{false, nil, position}}
}

func (t *Log) BeginTransactionLog(filename string, tid TransactionID) *LogRecord {
	lsn := NewLSN()
	t.transaction_table[tid] = lsn

	return &LogRecord{filename, lsn, tid, nil, LogBeginTransaction, nil, nil}
}

func (t *Log) CommitTransactionLog(filename string, tid TransactionID) *LogRecord {
	lsn := NewLSN()
	prev_lsn := t.transaction_table[tid]
	delete(t.transaction_table, tid)

	return &LogRecord{filename, lsn, tid, prev_lsn, LogCommitTransaction, nil, nil}
}

func (t *Log) AbortTransactionLog(filename string, tid TransactionID) *LogRecord {
	lsn := NewLSN()
	prev_lsn := t.transaction_table[tid]
	delete(t.transaction_table, tid)

	return &LogRecord{filename, lsn, tid, prev_lsn, LogAbortTransaction, nil, nil}
}

func (t *Log) Append(record *LogRecord) error {
	buf := new(bytes.Buffer)
	record.writeTo(buf)

	n, err := t.file.Write(buf.Bytes())
	if n != buf.Len() {
		return fmt.Errorf("Did not write full buffer to file")
	}
	return err
}
