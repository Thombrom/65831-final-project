package godb

// TODO: some code goes here
type InsertOp struct {
	// TODO: some code goes here
	file DBFile
	op   Operator
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{insertFile, child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	ft := FieldType{"count", "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	count := 0
	iter, err := iop.op.Iterator(tid)
	if err != nil {
		return nil, err
	}

	for tuple, err := iter(); tuple != nil || err != nil; tuple, err = iter() {
		count += 1

		err = iop.file.insertTuple(tuple, tid)
		if err != nil {
			return nil, err
		}
	}

	return func() (*Tuple, error) {
		tuple := Tuple{Desc: *(iop.Descriptor()), Fields: []DBValue{IntField{int64(count)}}, Rid: 0}
		return &tuple, nil
	}, nil
}
