package godb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
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
	checkpoint_offset        int64
	records_since_checkpoint int64

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
		decoder := gob.NewDecoder(bbuf)
		err := decoder.Decode(&mapping)

		if err != nil {
			return make(map[heapHash]int)
		}
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
		decoder := gob.NewDecoder(bbuf)
		err := decoder.Decode(&mapping)

		if err != nil {
			return make(map[int]int)
		}
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
	file, err := os.OpenFile(fromfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		return nil, err
	}

	dirty_page_table := recover_dirty_page_table(fromfile)
	transaction_table := recover_transaction_table(fromfile)
	checkpoint := recover_checkpoint(fromfile)
	fmt.Println(dirty_page_table, transaction_table, checkpoint)

	return &Log{fromfile, file, transaction_table, dirty_page_table, checkpoint, 0, sync.Mutex{}}, nil
}

func (t *Log) Close() error {
	return t.file.Close()
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

func reverse[T any](s []T) []T {
	rev := make([]T, 0)

	for i := len(s) - 1; i >= 0; i-- {
		rev = append(rev, s[i])
	}

	return rev
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
	aborted_tids := make(map[int]int)

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
		// fmt.Println("Redo metadata: ", metadata)

		if metadata.optype == LogAbortTransaction {
			aborted_tids[*metadata.tid] = 0
		}

		if metadata.optype != LogInsertDelete {
			continue
		}

		if *metadata.lsn < min_reclsn {
			break
		}

		_, in_dirty_pages := t.dirty_page_table[metadata.hh]
		_, got_aborted := aborted_tids[*metadata.tid]
		if in_dirty_pages && !got_aborted {
			result = append(result, reader.GetOffset())
		}
	}

	return reverse(result), nil
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

// Main recovery function. This should be called on the log right after
// creating it, taking as input all the heappages
func (t *Log) Recover(heapfiles []*HeapFile) error {
	filename_mapping := make(map[string]*HeapFile)
	for _, file := range heapfiles {
		filename, err := file.GetFilename()
		if err != nil {
			return err
		}

		filename_mapping[filename] = file
	}

	// First we recover state
	t.RecoverState()

	// Then we redo all nessecary changes
	redo_offsets, err := t.get_redo_record_offsets()
	if err != nil {
		return err
	}

	reader, err := t.CreateLogReaderAtCheckpoint()
	if err != nil {
		return err
	}

	for _, offset := range redo_offsets {
		reader.SetOffset(offset)
		metadata, err := reader.ReadMetadata()
		if err != nil {
			return err
		}

		file, ok := filename_mapping[metadata.hh.FileName]
		if !ok {
			return fmt.Errorf("Unknown filename %s", metadata.hh.FileName)
		}

		logrecord, err := reader.ReadLogRecord(file.Descriptor())
		if err != nil {
			return err
		}

		page, err := file.readPage(metadata.hh.PageNo)
		if err != nil {
			return err
		}

		heappage := (*page).(*heapPage)
		fmt.Println("Redo - Inserting ", logrecord.redo.tuple, " into ", metadata.hh.FileName, " on page ", metadata.hh.PageNo, " in slot ", logrecord.redo.pd.SlotNo)
		heappage.insertTupleAt(logrecord.redo.tuple, int(logrecord.redo.pd.SlotNo))
		file.flushPage(page)
	}

	// Now we undo actions
	undo_offsets, err := t.get_undo_record_offsets()
	if err != nil {
		return err
	}

	for _, offset := range undo_offsets {
		reader.SetOffset(offset)

		metadata, err := reader.ReadMetadata()
		if err != nil {
			return err
		}

		file, ok := filename_mapping[metadata.hh.FileName]
		if !ok {
			return fmt.Errorf("Unknown filename %s", metadata.hh.FileName)
		}

		logrecord, err := reader.ReadLogRecord(file.Descriptor())
		if err != nil {
			return err
		}

		page, err := file.readPage(metadata.hh.PageNo)
		if err != nil {
			return err
		}

		heappage := (*page).(*heapPage)
		fmt.Println("Undo - Inserting ", logrecord.undo.tuple, " into ", metadata.hh.FileName, " on page ", metadata.hh.PageNo, " in slot ", logrecord.redo.pd.SlotNo)
		heappage.insertTupleAt(logrecord.undo.tuple, int(logrecord.redo.pd.SlotNo))
		err = file.flushPage(page)
		if err != nil {
			return err
		}

	}

	// Abort all rolled back transactions
	for tid := range t.transaction_table {
		t.Append(AbortTransactionLog(&tid))
	}

	// We have aborted all existing transactions at the time of the crash
	// and we have flushed all recovered state to disc, thus we can clear
	// the datastructures
	for hh := range t.dirty_page_table {
		delete(t.dirty_page_table, hh)
	}

	for k := range t.transaction_table {
		delete(t.transaction_table, k)
	}

	t.CheckpointL()
	return nil
}

func (t *Log) CheckpointL() error {

	{ // Transaction table
		filename := transaction_table_filename(t.fromfile)
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return err
		}

		bbuf := new(bytes.Buffer)
		encoder := gob.NewEncoder(bbuf)
		err = encoder.Encode(t.transaction_table)
		if err != nil {
			return err
		}

		_, err = file.Write(bbuf.Bytes())
		if err != nil {
			return err
		}
	}

	{ // Transaction table
		filename := dirty_page_table_filename(t.fromfile)
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return err
		}

		bbuf := new(bytes.Buffer)
		encoder := gob.NewEncoder(bbuf)
		err = encoder.Encode(t.dirty_page_table)
		if err != nil {
			return err
		}

		_, err = file.Write(bbuf.Bytes())
		if err != nil {
			return err
		}
	}

	{ // Checkpoint location
		filename := checkpoint_filename(t.fromfile)
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return err
		}

		buf := make([]byte, 0)
		bbuf := bytes.NewBuffer(buf)

		stat, err := t.file.Stat()
		if err != nil {
			return err
		}

		err = binary.Write(bbuf, binary.LittleEndian, stat.Size())
		if err != nil {
			return err
		}
		_, err = file.Write(bbuf.Bytes())
		if err != nil {
			return err
		}
	}

	t.records_since_checkpoint = 0
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

	// stat, _ := t.file.Stat()
	// fmt.Println("Appending ", buf.Len(), " bytes to ", stat.Size())

	n, err := t.file.Write(buf.Bytes())
	// fmt.Println(n, err)
	if n != buf.Len() {
		return fmt.Errorf("Did not write full buffer to file")
	}

	if err != nil {
		return err
	}

	t.records_since_checkpoint += 1
	if t.records_since_checkpoint > 10 {
		err := t.CheckpointL()
		if err != nil {
			fmt.Println("Error: ", err.Error())
			return err
		}
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

	// fmt.Println("Length of prev: ", length)
	return int64(length), nil
}

func (t *LogReader) GetBuffer() ([]byte, error) {
	length, err := t.LengthNext()
	// fmt.Println("Getting buffer. Offset: ", t.offset, ", Length: ", length)
	// fmt.Println("length: ", length, t.offset)
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
