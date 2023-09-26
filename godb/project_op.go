package godb

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator

	// Not supporting child right now
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	return &Project{selectFields, outputNames, child}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	fields := make([]FieldType, 0)

	for i := 0; i < len(p.outputNames); i++ {
		field := FieldType{p.outputNames[i], "", p.selectFields[i].GetExprType().Ftype}
		fields = append(fields, field)
	}

	return &TupleDesc{fields}
}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, err := p.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	return func() (*Tuple, error) {
		val, err := iter()
		if err != nil || val == nil {
			return val, err
		}

		vals := make([]DBValue, 0)
		for _, expr := range p.selectFields {
			tupval, err := expr.EvalExpr(val)
			if err != nil {
				return nil, err
			}

			vals = append(vals, tupval)
		}

		return &Tuple{*p.Descriptor(), vals, 0}, nil
	}, nil
}
