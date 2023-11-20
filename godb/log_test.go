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
	err = log.Append(log.BeginTransactionLog(tid))
	if err != nil {
		t.Fatalf(err.Error())
	}

	logrecord_insert := log.InsertLog("table.csv", tid, PositionDescriptor{0, 0}, &t1)
	if (*log.get_transaction_table())[*tid] != *logrecord_insert.metadata.lsn {
		t.Fatalf("Transaction table not reflecting most recent lsn")
	}
	if (*log.get_dirty_page_table())[heapHash{"table.csv", 0}] != *logrecord_insert.metadata.lsn {
		t.Fatalf("Dirty page table does not reflect first dirtying of page")
	}

	err = log.Append(logrecord_insert)
	if err != nil {
		t.Fatalf(err.Error())
	}

	logrecord_delete := log.DeleteLog("table.csv", tid, PositionDescriptor{0, 0}, &t1)
	if (*log.get_transaction_table())[*tid] != *logrecord_delete.metadata.lsn {
		t.Fatalf("Transaction table not reflecting most recent lsn")
	}
	if (*log.get_dirty_page_table())[heapHash{"table.csv", 0}] != *logrecord_insert.metadata.lsn {
		t.Fatalf("Dirty page table does not reflect first dirtying of page")
	}

	err = log.Append(logrecord_delete)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = log.Append(log.AbortTransactionLog(tid))
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
	records[0] = log.BeginTransactionLog(tid)
	records[1] = log.InsertLog("table.csv", tid, PositionDescriptor{0, 0}, &t1)
	records[2] = log.DeleteLog("table.csv", tid, PositionDescriptor{0, 0}, &t1)
	records[3] = log.AbortTransactionLog(tid)

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

func TestLogRecovery(t *testing.T) {
	ClearLog()
	_, t1, _, _, _, _ := makeTestVars()
	log, err := newLog(TestingFileLog)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Add a transaction of insert then delete then abort to the log
	tid := NewTID()

	records := make([]*LogRecord, 4)
	records[0] = log.BeginTransactionLog(tid)
	records[1] = log.InsertLog("table.csv", tid, PositionDescriptor{0, 0}, &t1)
	records[2] = log.DeleteLog("table.csv", tid, PositionDescriptor{0, 0}, &t1)
	records[3] = log.AbortTransactionLog(tid)

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
