package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
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
	transaction_table map[int]int

	// Maps each pageno to the log record that first
	// dirtied that page
	dirty_page_table map[heapHash]int

	// The offset into the log file for the last checkpoint
	// This value is used for faster recovery
	checkpoint_offset int64

	mutex sync.Mutex
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

type LogRecordMetadata struct {
	// Filename is an empty string when optype != InsertDelete
	hh       heapHash
	lsn      LogSequenceNumber
	tid      TransactionID
	prev_lsn LogSequenceNumber
	optype   OperationType
}

// Log records are serialized with the size of the
// log record as the first bytes, then the string
// encoded with the filename length as the first
// value followed by those bytes to allow for variable
// length filenames, and knowing which records are
// for which heapfiles
type LogRecord struct {
	metadata LogRecordMetadata

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

func recover_dirty_page_table(fromfile string) map[heapHash]int {
	filename := dirty_page_table_filename(fromfile)
	mapping := make(map[heapHash]int)

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

func recover_transaction_table(fromfile string) map[int]int {
	filename := transaction_table_filename(fromfile)
	mapping := make(map[int]int)

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
	file, err := os.OpenFile(fromfile, os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return nil, err
	}

	dirty_page_table := recover_dirty_page_table(fromfile)
	transaction_table := recover_transaction_table(fromfile)
	checkpoint := recover_checkpoint(fromfile)

	return &Log{fromfile, file, transaction_table, dirty_page_table, checkpoint, sync.Mutex{}}, nil
}

func (t *Log) get_dirty_page_table() *map[heapHash]int {
	return &t.dirty_page_table
}

func (t *Log) get_transaction_table() *map[int]int {
	return &t.transaction_table
}

// Recovers state after a crash
func (t *Log) RecoverState() error {
	// When the log is initalized from file, it loads in the
	// last stored dirty page table and transaction table, as well
	// as checkpoint

	reader, err := t.CreateLogReaderAtCheckpoint()
	if err != nil {
		return err
	}

	for !reader.AtEnd() {
		metadata, err := reader.ReadMetadata()
		if err != nil {
			return err
		}

		switch metadata.optype {
		case LogBeginTransaction:
			t.transaction_table[*metadata.tid] = *metadata.lsn

		case LogAbortTransaction:
			delete(t.transaction_table, *metadata.tid)

		case LogCommitTransaction:
			delete(t.transaction_table, *metadata.tid)

		case LogInsertDelete:
			t.transaction_table[*metadata.tid] = *metadata.lsn
			_, ok := t.dirty_page_table[metadata.hh]
			if !ok {
				t.dirty_page_table[metadata.hh] = *metadata.lsn
			}
		}

		reader.AdvanceNext()
	}

	return nil
}

// Returns a list of offsets into the log which should be redone
// These are records with the same heaphash as the ones found in
// the dirty pages table after recovering state
func (t *Log) get_redo_record_offsets() ([]int64, error) {
	reader, err := t.CreateLogReaderAtEnd()
	if err != nil {
		return nil, err
	}
	result := make([]int64, 0)

	// If no dirty pages, we can quit early
	if len(t.dirty_page_table) == 0 {
		return result, nil
	}

	min_reclsn := 2147483647
	for _, reclsn := range t.dirty_page_table {
		if reclsn < int(min_reclsn) {
			min_reclsn = reclsn
		}
	}

	for !reader.AtStart() {
		err = reader.AdvancePrev()
		if err != nil {
			return nil, err
		}

		metadata, err := reader.ReadMetadata()
		if err != nil {
			return nil, err
		}

		if metadata.optype != LogInsertDelete {
			continue
		}

		if *metadata.lsn < min_reclsn {
			break
		}

		_, ok := t.dirty_page_table[metadata.hh]
		if ok {
			result = append(result, reader.GetOffset())
		}
	}

	return result, nil
}

// Returns a list of all the records that need to be undone.
// A record needs to be undone if it is in the transactions table
// after having recovered state.
func (t *Log) get_undo_record_offsets() ([]int64, error) {
	tids_to_look_for := make(map[int]int)
	for tid := range t.transaction_table {
		tids_to_look_for[tid] = 0
	}

	reader, err := t.CreateLogReaderAtEnd()
	if err != nil {
		return nil, err
	}

	result := make([]int64, 0)

	for !reader.AtStart() && len(tids_to_look_for) != 0 {
		err = reader.AdvancePrev()
		if err != nil {
			return nil, err
		}

		metadata, err := reader.ReadMetadata()
		if err != nil {
			return nil, err
		}

		// If we encounter a begintransaction of one that is to be undone, we remove it from
		// the list of tids to look for as we have gotten all of those offsets now
		if _, ok := tids_to_look_for[*metadata.tid]; ok && metadata.optype == LogBeginTransaction {
			delete(tids_to_look_for, *metadata.tid)
		}

		if _, ok := tids_to_look_for[*metadata.tid]; ok && metadata.optype == LogInsertDelete {
			result = append(result, reader.GetOffset())
		}
	}

	return result, nil
}

func (t *Log) recovery_abort_lost_transactions() error {
	for tid := range t.transaction_table {
		err := t.Append(AbortTransactionLog(&tid))
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *LogRecordMetadata) writeTo(b *bytes.Buffer) error {
	err := errors.Join(
		// Record filename
		binary.Write(b, binary.LittleEndian, int64(len(t.hh.FileName))),
		binary.Write(b, binary.LittleEndian, []byte(t.hh.FileName)),

		// Record next items
		binary.Write(b, binary.LittleEndian, int64(t.hh.PageNo)),
		binary.Write(b, binary.LittleEndian, int64(*t.lsn)),
		binary.Write(b, binary.LittleEndian, int64(*t.tid)),
		binary.Write(b, binary.LittleEndian, int64(t.optype)),
	)

	if t.optype != LogBeginTransaction {
		binary.Write(b, binary.LittleEndian, int64(*t.prev_lsn))
	}

	return err
}

func (t *LogRecord) writeTo(b *bytes.Buffer) error {
	sub_buf := new(bytes.Buffer)
	t.metadata.writeTo(sub_buf)

	// Record undo and redo
	if t.metadata.optype == LogInsertDelete {
		err := errors.Join(
			t.undo.writeTo(sub_buf),
			t.redo.writeTo(sub_buf),
		)

		if err != nil {
			return err
		}
	}

	// Record the size as the first value followed by the bytes
	err := binary.Write(b, binary.LittleEndian, int64(len(sub_buf.Bytes())+16))
	if err != nil {
		return err
	}

	_, err = b.Write((sub_buf.Bytes()))
	if err != nil {
		return err
	}

	// We also write it after the log such that we can go back in the buffer
	err = binary.Write(b, binary.LittleEndian, int64(len(sub_buf.Bytes())+16))
	return err
}

func (t *LogRecordMetadata) equals(other *LogRecordMetadata) bool {
	eq := t.hh.FileName == other.hh.FileName &&
		t.hh.PageNo == other.hh.PageNo &&
		*t.lsn == *other.lsn &&
		*t.tid == *other.tid &&
		t.optype == other.optype

	if t.optype != LogBeginTransaction {
		eq = eq && *t.prev_lsn == *other.prev_lsn
	}

	return eq
}

func (t *LogRecord) equals(other *LogRecord) bool {
	eq := t.metadata.equals(&other.metadata)

	if t.metadata.optype == LogInsertDelete {
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

func ReadLogMetadataFrom(b *bytes.Buffer) (*LogRecordMetadata, error) {
	filename_length, err := read_int_log(b)
	if err != nil {
		return nil, err
	}

	filename, err := read_string_log(b, int(filename_length))
	if err != nil {
		return nil, err
	}

	pageno, err := read_int_log(b)
	if err != nil {
		return nil, err
	}

	lsn, err1 := read_int_log(b)
	tid, err2 := read_int_log(b)
	optype, err4 := read_int_log(b)
	optype_typed := OperationType(optype)

	err = errors.Join(err1, err2, err4)
	if err != nil {
		return nil, err
	}

	var prev_lsn LogSequenceNumber
	if optype_typed != LogBeginTransaction {
		prev_lsn_val, err := read_int_log(b)
		if err != nil {
			return nil, err
		}

		prev_lsn = &prev_lsn_val
	}

	return &LogRecordMetadata{heapHash{filename, pageno}, &lsn, &tid, prev_lsn, optype_typed}, nil
}

func ReadLogRecordFrom(b *bytes.Buffer, desc *TupleDesc) (*LogRecord, error) {
	length, err := read_int_log(b)
	if err != nil {
		return nil, err
	}

	metadata, err := ReadLogMetadataFrom(b)
	if err != nil {
		return nil, err
	}

	if metadata.optype != LogInsertDelete {
		return &LogRecord{*metadata, nil, nil}, nil
	}

	undo, err := ReadLogOperationFrom(b, desc)
	if err != nil {
		return nil, err
	}

	redo, err := ReadLogOperationFrom(b, desc)
	if err != nil {
		return nil, err
	}

	length_post, err := read_int_log(b)
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	if length != length_post {
		return nil, fmt.Errorf("Pre and post length mismatch")
	}

	return &LogRecord{*metadata, undo, redo}, nil
}

// TODO: Mutex
func InsertLog(filename string, tid TransactionID, position PositionDescriptor, new_tuple *Tuple) *LogRecord {
	lsn := NewLSN()
	return &LogRecord{LogRecordMetadata{heapHash{filename, int(position.PageNo)}, lsn, tid, nil, LogInsertDelete}, &LogOperation{false, nil, position}, &LogOperation{true, new_tuple, position}}
}

func DeleteLog(filename string, tid TransactionID, position PositionDescriptor, prev_tuple *Tuple) *LogRecord {
	lsn := NewLSN()
	return &LogRecord{LogRecordMetadata{heapHash{filename, int(position.PageNo)}, lsn, tid, nil, LogInsertDelete}, &LogOperation{true, prev_tuple, position}, &LogOperation{false, nil, position}}
}

func BeginTransactionLog(tid TransactionID) *LogRecord {
	lsn := NewLSN()
	return &LogRecord{LogRecordMetadata{heapHash{"", 0}, lsn, tid, nil, LogBeginTransaction}, nil, nil}
}

func CommitTransactionLog(tid TransactionID) *LogRecord {
	lsn := NewLSN()
	return &LogRecord{LogRecordMetadata{heapHash{"", 0}, lsn, tid, nil, LogCommitTransaction}, nil, nil}
}

func AbortTransactionLog(tid TransactionID) *LogRecord {
	lsn := NewLSN()
	return &LogRecord{LogRecordMetadata{heapHash{"", 0}, lsn, tid, nil, LogAbortTransaction}, nil, nil}
}

func (t *Log) Append(record *LogRecord) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	switch record.metadata.optype {
	case LogBeginTransaction:
		t.transaction_table[*record.metadata.tid] = *record.metadata.lsn

	case LogCommitTransaction:
		prev_lsn := t.transaction_table[*record.metadata.tid]
		record.metadata.prev_lsn = &prev_lsn
		t.transaction_table[*record.metadata.tid] = *record.metadata.lsn
		delete(t.transaction_table, *record.metadata.tid)

	case LogAbortTransaction:
		prev_lsn := t.transaction_table[*record.metadata.tid]
		record.metadata.prev_lsn = &prev_lsn
		t.transaction_table[*record.metadata.tid] = *record.metadata.lsn
		delete(t.transaction_table, *record.metadata.tid)

	case LogInsertDelete:
		prev_lsn := t.transaction_table[*record.metadata.tid]
		record.metadata.prev_lsn = &prev_lsn
		t.transaction_table[*record.metadata.tid] = *record.metadata.lsn

		_, ok := t.dirty_page_table[record.metadata.hh]
		if !ok {
			t.dirty_page_table[record.metadata.hh] = *record.metadata.lsn
		}
	}

	buf := new(bytes.Buffer)
	record.writeTo(buf)

	n, err := t.file.Write(buf.Bytes())
	if n != buf.Len() {
		return fmt.Errorf("Did not write full buffer to file")
	}
	return err
}

type LogReader struct {
	reader *bytes.Reader
	offset int64
}

func (t *Log) CreateLogReaderAtCheckpoint() (*LogReader, error) {
	reader, err := t.CreateLogReaderAtEnd()
	if err != nil {
		return nil, err
	}

	reader.SetOffset(t.checkpoint_offset)
	return reader, nil
}

func (t *Log) CreateLogReaderAtEnd() (*LogReader, error) {
	file, err := os.Open(t.fromfile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, stat.Size())
	_, err = file.Read(buf)
	if err != nil {
		return nil, err
	}

	bbuf := bytes.NewReader(buf)
	return &LogReader{bbuf, stat.Size()}, nil
}

func (t *LogReader) AtEnd() bool {
	return t.reader.Len() == int(t.offset)
}

func (t *LogReader) AtStart() bool {
	return t.offset == 0
}

func (t *LogReader) GetOffset() int64 {
	return t.offset
}

func (t *LogReader) SetOffset(offset int64) {
	t.offset = offset
}

func (t *LogReader) LengthNext() (int64, error) {
	if t.AtEnd() {
		return 0, fmt.Errorf("At end")
	}

	lengthbuf := make([]byte, 8)
	_, err := t.reader.ReadAt(lengthbuf, t.offset)
	if err != nil {
		return 0, err
	}

	length, err := read_int_log(bytes.NewBuffer(lengthbuf))
	if err != nil {
		return 0, err
	}

	return int64(length), nil
}

func (t *LogReader) LengthPrev() (int64, error) {
	if t.offset == 0 {
		return 0, fmt.Errorf("At start")
	}

	lengthbuf := make([]byte, 8)
	_, err := t.reader.ReadAt(lengthbuf, t.offset-8)
	if err != nil {
		return 0, err
	}

	length, err := read_int_log(bytes.NewBuffer(lengthbuf))
	if err != nil {
		return 0, err
	}

	return int64(length), nil
}

func (t *LogReader) GetBuffer() ([]byte, error) {
	length, err := t.LengthNext()
	recordbuf := make([]byte, length)
	_, err = t.reader.ReadAt(recordbuf, t.offset)
	if err != nil {
		return nil, err
	}

	return recordbuf, nil
}

func (t *LogReader) ReadMetadata() (*LogRecordMetadata, error) {
	recordbuf, err := t.GetBuffer()
	if err != nil {
		return nil, err
	}

	// We skip the first 8 bytes as these are for the length
	record, err := ReadLogMetadataFrom(bytes.NewBuffer(recordbuf[8:]))
	return record, err
}

func (t *LogReader) ReadLogRecord(desc *TupleDesc) (*LogRecord, error) {
	recordbuf, err := t.GetBuffer()
	if err != nil {
		return nil, err
	}

	record, err := ReadLogRecordFrom(bytes.NewBuffer(recordbuf), desc)
	return record, err
}

func (t *LogReader) AdvanceNext() error {
	length, err := t.LengthNext()
	if err != nil {
		return err
	}

	t.offset += length
	return nil
}

func (t *LogReader) AdvancePrev() error {
	length, err := t.LengthPrev()
	if err != nil {
		return err
	}

	t.offset -= length
	return nil
}
