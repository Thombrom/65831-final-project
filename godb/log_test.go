package godb

import (
	"bytes"
	"fmt"
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

		undo: &LogOperation{tuple: &t1, pd: PositionDescriptor{0, 0}, operation: OperationWrite},
		redo: &LogOperation{tuple: &t2, pd: PositionDescriptor{0, 0}, operation: OperationWrite},
	}

	buffer := new(bytes.Buffer)
	err := log_record.writeTo(buffer)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(buffer.Len())

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

		undo: &LogOperation{tuple: nil, pd: PositionDescriptor{0, 0}, operation: OperationWrite},
		redo: &LogOperation{tuple: nil, pd: PositionDescriptor{0, 0}, operation: OperationDelete},
	}

	buffer = new(bytes.Buffer)
	err = log_record.writeTo(buffer)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(buffer.Len())

	log_record_read, err = ReadLogRecordFrom(buffer, &td)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if !log_record.equals(log_record_read) {
		t.Fatalf("Comparison of log records did not equal")
	}
}
