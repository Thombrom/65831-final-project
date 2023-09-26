package godb

import (
	"os"
)

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not and integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.
func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {
	os.Remove("lab1query.dat")
	bp := NewBufferPool(16)
	heap_file, err := NewHeapFile("lab1query.dat", &td, bp)
	if err != nil {
		return 0, err
	}

	file, err := os.Open(fileName)
	if err != nil {
		return 0, err
	}

	heap_file.LoadFromCSV(file, true, ",", false)
	idx, err := findFieldInTd(FieldType{Fname: sumField, Ftype: IntType, TableQualifier: ""}, &td)
	if err != nil {
		return 0, err
	}

	f := FieldExpr{td.Fields[idx]}
	iter, err := heap_file.Iterator(NewTID())
	if err != nil {
		return 0, err
	}

	sum := 0
	for {
		tuple, err := iter()
		if err != nil {
			return 0, err
		}

		if tuple == nil {
			return sum, nil
		}

		value, err := f.EvalExpr(tuple)
		if err != nil {
			return 0, err
		}

		sum += int(value.(IntField).Value)
	}
}
