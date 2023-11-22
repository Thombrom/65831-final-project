package godb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
)

const TestingFileLog string = "test_logging"

func ClearLog() {
	os.Remove(dirty_page_table_filename(TestingFileLog))
	os.Remove(transaction_table_filename(TestingFileLog))
	os.Remove(checkpoint_filename(TestingFileLog))
	os.Remove(TestingFileLog)
}

func TestLogSerializeDeserialize(t *testing.T) {
	ClearLog()
	td, t1, t2, _, _, _ := makeTestVars()

	log_record := &LogRecord{
		metadata: LogRecordMetadata{
			hh:       heapHash{"table.csv", 0},
			prev_lsn: NewLSN(),
			lsn:      NewLSN(),
			tid:      NewTID(),
			optype:   LogInsertDelete,
		},
		undo: &LogOperation{tuple: &t1, pd: PositionDescriptor{0, 1}},
		redo: &LogOperation{tuple: &t2, pd: PositionDescriptor{1, 0}},
	}

	buffer := new(bytes.Buffer)
	err := log_record.writeTo(buffer)
	if err != nil {
		t.Fatalf(err.Error())
	}

	log_record_read, err := ReadLogRecordFrom(buffer, &td)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if !log_record.equals(log_record_read) {
		t.Fatalf("Comparison of log records did not equal")
	}

	// Test that it can support the tuples being nil as well
	log_record = &LogRecord{
		metadata: LogRecordMetadata{
			hh:       heapHash{"table.csv", 0},
			prev_lsn: NewLSN(),
			lsn:      NewLSN(),
			tid:      NewTID(),
			optype:   LogInsertDelete,
		},

		undo: &LogOperation{tuple: nil, pd: PositionDescriptor{1, 0}},
		redo: &LogOperation{tuple: nil, pd: PositionDescriptor{0, 1}},
	}

	buffer = new(bytes.Buffer)
	err = log_record.writeTo(buffer)
	if err != nil {
		t.Fatalf(err.Error())
	}

	log_record_read, err = ReadLogRecordFrom(buffer, &td)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if !log_record.equals(log_record_read) {
		t.Fatalf("Comparison of log records did not equal")
	}

	// Check non-insert-delete type
	log_record = &LogRecord{
		metadata: LogRecordMetadata{
			hh:       heapHash{"table.csv", 0},
			prev_lsn: NewLSN(),
			lsn:      NewLSN(),
			tid:      NewTID(),
			optype:   LogAbortTransaction,
		},
		undo: nil,
		redo: nil,
	}

	buffer = new(bytes.Buffer)
	err = log_record.writeTo(buffer)
	if err != nil {
		t.Fatalf(err.Error())
	}

	log_record_read, err = ReadLogRecordFrom(buffer, &td)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestLogTransactionTable(t *testing.T) {
	ClearLog()
	_, t1, _, _, _, _ := makeTestVars()
	log, err := newLog(TestingFileLog)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Add a transaction of insert then delete then abort to the log
	tid := NewTID()
	err = log.Append(BeginTransactionLog(tid))
	if err != nil {
		t.Fatalf(err.Error())
	}

	logrecord_insert := InsertLog("table.csv", tid, PositionDescriptor{0, 0}, &t1)
	err = log.Append(logrecord_insert)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if (*log.get_transaction_table())[*tid] != *logrecord_insert.metadata.lsn {
		t.Fatalf("Transaction table not reflecting most recent lsn")
	}
	if (*log.get_dirty_page_table())[heapHash{"table.csv", 0}] != *logrecord_insert.metadata.lsn {
		t.Fatalf("Dirty page table does not reflect first dirtying of page")
	}

	logrecord_delete := DeleteLog("table.csv", tid, PositionDescriptor{0, 0}, &t1)
	err = log.Append(logrecord_delete)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if (*log.get_transaction_table())[*tid] != *logrecord_delete.metadata.lsn {
		t.Fatalf("Transaction table not reflecting most recent lsn")
	}
	if (*log.get_dirty_page_table())[heapHash{"table.csv", 0}] != *logrecord_insert.metadata.lsn {
		t.Fatalf("Dirty page table does not reflect first dirtying of page")
	}

	err = log.Append(AbortTransactionLog(tid))
	if err != nil {
		t.Fatalf(err.Error())
	}

	_, ok := (*log.get_transaction_table())[*tid]
	if ok {
		t.Fatalf("Transaction table for tid should be cleared")
	}
}

func TestLogReader(t *testing.T) {
	ClearLog()
	_, t1, _, _, _, _ := makeTestVars()
	log, err := newLog(TestingFileLog)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Add a transaction of insert then delete then abort to the log
	tid := NewTID()

	records := make([]*LogRecord, 4)
	records[0] = BeginTransactionLog(tid)
	records[1] = InsertLog("table.csv", tid, PositionDescriptor{0, 0}, &t1)
	records[2] = DeleteLog("table.csv", tid, PositionDescriptor{0, 0}, &t1)
	records[3] = AbortTransactionLog(tid)

	err = errors.Join(
		log.Append(records[0]), log.Append(records[1]), log.Append(records[2]), log.Append(records[3]),
	)
	if err != nil {
		t.Fatalf("Error adding to log")
	}

	reader, err := log.CreateLogReaderAtCheckpoint()
	if err != nil {
		t.Fatalf(err.Error())
	}
	read_metadata, err := reader.ReadMetadata()
	reader.AdvanceNext()
	if err != nil || !read_metadata.equals(&records[0].metadata) {
		fmt.Println(read_metadata)
		t.Fatalf("Error in comparing read metadata, %s", err.Error())
	}

	read_metadata, err = reader.ReadMetadata()
	reader.AdvanceNext()
	if err != nil || !read_metadata.equals(&records[1].metadata) {
		t.Fatalf("Error in comparing read metadata")
	}

	read_metadata, err = reader.ReadMetadata()
	reader.AdvanceNext()
	if err != nil || !read_metadata.equals(&records[2].metadata) {
		t.Fatalf("Error in comparing read metadata")
	}

	read_metadata, err = reader.ReadMetadata()
	reader.AdvanceNext()
	if err != nil || !read_metadata.equals(&records[3].metadata) {
		t.Fatalf("Error in comparing read metadata")
	}

	if !reader.AtEnd() {
		t.Fatalf("Reader should be at end now")
	}
}

func TestLogRecoverState(t *testing.T) {
	ClearLog()
	_, t1, _, _, _, _ := makeTestVars()
	log, err := newLog(TestingFileLog)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Add a transaction of insert then delete then abort to the log
	tid := NewTID()

	records := make([]*LogRecord, 4)
	records[0] = BeginTransactionLog(tid)
	records[1] = InsertLog("table.csv", tid, PositionDescriptor{0, 0}, &t1)
	records[2] = DeleteLog("table.csv", tid, PositionDescriptor{0, 0}, &t1)
	records[3] = AbortTransactionLog(tid)

	err = errors.Join(
		log.Append(records[0]), log.Append(records[1]), log.Append(records[2]), log.Append(records[3]),
	)
	if err != nil {
		t.Fatalf("Error adding to log")
	}

	recovery_log, err := newLog(TestingFileLog)
	if err != nil {
		t.Fatalf(err.Error())
	}
	recovery_log.RecoverState()

	if !reflect.DeepEqual(recovery_log.get_dirty_page_table(), log.get_dirty_page_table()) {
		t.Fatal("Dirty page tables are not equal", recovery_log.get_dirty_page_table(), log.get_dirty_page_table())
	}

	if !reflect.DeepEqual(recovery_log.get_dirty_page_table(), log.get_dirty_page_table()) {
		t.Fatal("Dirty page tables are not equal", recovery_log.get_dirty_page_table(), log.get_dirty_page_table())
	}
}

func TestLogFindRedo(t *testing.T) {
	ClearLog()
	_, t1, t2, _, _, _ := makeTestVars()
	log, err := newLog(TestingFileLog)
	if err != nil {
		t.Fatalf(err.Error())
	}

	tid1 := NewTID()
	tid2 := NewTID()
	tid3 := NewTID()

	records := [...]*LogRecord{
		BeginTransactionLog(tid1),
		InsertLog("a", tid1, PositionDescriptor{0, 0}, &t1),
		InsertLog("b", tid1, PositionDescriptor{0, 0}, &t2),
		BeginTransactionLog(tid3),
		InsertLog("c", tid1, PositionDescriptor{0, 0}, &t2),
		BeginTransactionLog(tid2),
		InsertLog("d", tid2, PositionDescriptor{0, 0}, &t1),
		CommitTransactionLog(tid1),
		InsertLog("a", tid2, PositionDescriptor{0, 0}, &t1),
		CommitTransactionLog(tid2),
		InsertLog("e", tid3, PositionDescriptor{0, 0}, &t2),
	}

	for _, record := range records {
		err = log.Append(record)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	recovery_log, err := newLog(TestingFileLog)
	if err != nil {
		t.Fatalf(err.Error())
	}
	recovery_log.RecoverState()

	offsets, err := recovery_log.get_redo_record_offsets()
	if err != nil {
		t.Fatalf(err.Error())
	}
	expected := [...]int{
		*records[1].metadata.lsn, *records[2].metadata.lsn, *records[4].metadata.lsn,
		*records[6].metadata.lsn, *records[8].metadata.lsn, *records[10].metadata.lsn,
	}

	reader, err := recovery_log.CreateLogReaderAtEnd()
	if err != nil {
		t.Fatalf(err.Error())
	}
	for idx, offset := range offsets {
		reader.SetOffset(offset)
		metadata, err := reader.ReadMetadata()
		if err != nil {
			t.Fatalf(err.Error())
		}

		if expected[idx] != *metadata.lsn {
			t.Fatalf("Redo LSNS does not match")
		}
	}
}

func TestLogFindUndo(t *testing.T) {
	ClearLog()
	_, t1, t2, _, _, _ := makeTestVars()
	log, err := newLog(TestingFileLog)
	if err != nil {
		t.Fatalf(err.Error())
	}

	tid1 := NewTID()
	tid2 := NewTID()
	tid3 := NewTID()

	records := [...]*LogRecord{
		BeginTransactionLog(tid1),
		InsertLog("a", tid1, PositionDescriptor{0, 0}, &t1),
		InsertLog("b", tid1, PositionDescriptor{0, 0}, &t2),
		BeginTransactionLog(tid3),
		InsertLog("c", tid1, PositionDescriptor{0, 0}, &t2),
		BeginTransactionLog(tid2),
		InsertLog("d", tid2, PositionDescriptor{0, 0}, &t1),
		CommitTransactionLog(tid1),
		InsertLog("a", tid2, PositionDescriptor{0, 0}, &t1),
		CommitTransactionLog(tid2),
		InsertLog("e", tid3, PositionDescriptor{0, 0}, &t2),
	}

	for _, record := range records {
		err = log.Append(record)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	recovery_log, err := newLog(TestingFileLog)
	if err != nil {
		t.Fatalf(err.Error())
	}
	recovery_log.RecoverState()

	offsets, err := recovery_log.get_undo_record_offsets()
	if err != nil {
		t.Fatalf(err.Error())
	}
	expected := [...]int{
		*records[10].metadata.lsn,
	}

	reader, err := recovery_log.CreateLogReaderAtEnd()
	if err != nil {
		t.Fatalf(err.Error())
	}
	for idx, offset := range offsets {
		reader.SetOffset(offset)
		metadata, err := reader.ReadMetadata()
		if err != nil {
			t.Fatalf(err.Error())
		}

		if expected[idx] != *metadata.lsn {
			t.Fatalf("Undo LSNS does not match")
		}
	}
}

func TestLogRecovery(t *testing.T) {
	ClearLog()
	_, t1, t2, hf, bp, tid := makeTestVars()

	// Make sure that we have a page 0 of the heapfile
	bp.BeginTransaction(tid)
	hf.insertTuple(&t1, tid)
	bp.CommitTransaction(tid)
	bp.FlushAllPages()

	log, err := newLog(TestingFileLog)
	if err != nil {
		t.Fatalf(err.Error())
	}

	hf_name, err := hf.GetFilename()
	if err != nil {
		t.Fatalf(err.Error())
	}

	tid1 := NewTID()
	tid2 := NewTID()
	tid3 := NewTID()

	records := [...]*LogRecord{
		BeginTransactionLog(tid1),
		InsertLog(hf_name, tid1, PositionDescriptor{0, 0}, &t1),
		InsertLog(hf_name, tid1, PositionDescriptor{0, 1}, &t2),
		BeginTransactionLog(tid3),
		InsertLog(hf_name, tid1, PositionDescriptor{0, 2}, &t2),
		BeginTransactionLog(tid2),
		InsertLog(hf_name, tid2, PositionDescriptor{0, 3}, &t1),
		CommitTransactionLog(tid1),
		InsertLog(hf_name, tid2, PositionDescriptor{0, 1}, &t1),
		CommitTransactionLog(tid2),
		InsertLog(hf_name, tid3, PositionDescriptor{0, 4}, &t2),
	}

	for _, record := range records {
		err = log.Append(record)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	recovery_log, err := newLog(TestingFileLog)
	if err != nil {
		t.Fatalf(err.Error())
	}

	hfs := []*HeapFile{hf}
	recovery_log.Recover(hfs)

	expected := [...]*Tuple{&t1, &t1, &t2, &t1}
	iter, err := hf.Iterator(NewTID())
	if err != nil {
		t.Fatalf(err.Error())
	}

	for _, exp := range expected {
		fmt.Println(exp)
		got, err := iter()
		if err != nil {
			t.Fatalf(err.Error())
		}

		if got == nil {
			t.Fatalf("Ran out of tuples")
		}

		if !exp.equals(got) {
			t.Fatalf("Tuple mismatch")
		}
	}

	got, err := iter()
	if got != nil || err != nil {
		t.Fatalf("More tuples than expected. Error!")
	}
}
