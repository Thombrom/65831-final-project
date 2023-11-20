package godb

import (
	"bytes"
	"os"
	"testing"
)

const TestingFileLog string = "test.dat"

func TestLogSerializeDeserialize(t *testing.T) {
	td, t1, t2, _, _, _ := makeTestVars()
	os.Remove(TestingFile)

	log_record := &LogRecord{
		filename: "table.csv",
		prev_lsn: NewLSN(),
		lsn:      NewLSN(),
		tid:      NewTID(),
		optype:   LogInsertDelete,

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
		filename: "table.csv",
		prev_lsn: NewLSN(),
		lsn:      NewLSN(),
		tid:      NewTID(),
		optype:   LogInsertDelete,

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
		filename: "table.csv",
		prev_lsn: NewLSN(),
		lsn:      NewLSN(),
		tid:      NewTID(),
		optype:   LogAbortTransaction,

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
