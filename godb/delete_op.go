package godb

type DeleteOp struct {
	// TODO: some code goes here
	file DBFile
	op   Operator
}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{deleteFile, child}
}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	ft := FieldType{"count", "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td

}

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	count := 0
	iter, err := dop.op.Iterator(tid)
	if err != nil {
		return nil, err
	}

	for tuple, err := iter(); tuple != nil || err != nil; tuple, err = iter() {
		count += 1

		err = dop.file.deleteTuple(tuple, tid)
		if err != nil {
			return nil, err
		}
	}

	return func() (*Tuple, error) {
		tuple := Tuple{Desc: *(dop.Descriptor()), Fields: []DBValue{IntField{int64(count)}}, Rid: 0}
		return &tuple, nil
	}, nil
}
