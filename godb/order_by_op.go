package godb

import (
	"sort"
)

// TODO: some code goes here
type OrderBy struct {
	orderBy   []Expr // OrderBy should include these two fields (used by parser)
	child     Operator
	ascending []bool
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	return &OrderBy{orderByFields, child, ascending}, nil
}

func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor()
}

// Implementation taken from https://pkg.go.dev/sort
type lessFunc func(p1, p2 DBValue) bool

// multiSorter implements the Sort interface, sorting the changes within.
type multiSorter struct {
	tuples []*Tuple
	exprs  []Expr
	less   []lessFunc
}

func (ms *multiSorter) Sort(tuples []*Tuple) {
	ms.tuples = tuples
	sort.Sort(ms)
}

// OrderedBy returns a Sorter that sorts using the less functions, in order.
// Call its Sort method to sort the data.
func OrderedBy(less []lessFunc, exprs []Expr) *multiSorter {
	return &multiSorter{
		less:  less,
		exprs: exprs,
	}
}

// Len is part of sort.Interface.
func (ms *multiSorter) Len() int {
	return len(ms.tuples)
}

// Swap is part of sort.Interface.
func (ms *multiSorter) Swap(i, j int) {
	ms.tuples[i], ms.tuples[j] = ms.tuples[j], ms.tuples[i]
}

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that discriminates between
// the two items (one is less than the other). Note that it can call the
// less functions twice per call. We could change the functions to return
// -1, 0, 1 and reduce the number of calls for greater efficiency: an
// exercise for the reader.
func (ms *multiSorter) Less(i, j int) bool {
	p, q := ms.tuples[i], ms.tuples[j]
	// Try all but the last comparison.
	var k int
	for k = 0; k < len(ms.less)-1; k++ {
		less := ms.less[k]
		pv, _ := ms.exprs[k].EvalExpr(p)
		qv, _ := ms.exprs[k].EvalExpr(q)

		switch {
		case less(pv, qv):
			// p < q, so we have a decision.
			return true
		case less(qv, pv):
			// p > q, so we have a decision.
			return false
		}
		// p == q; try the next comparison.
	}
	// All comparisons to here said "equal", so just return whatever
	// the final comparison reports.

	pv, _ := ms.exprs[k].EvalExpr(p)
	qv, _ := ms.exprs[k].EvalExpr(q)
	return ms.less[k](pv, qv)
}

func order_by_sub(op BoolOp, ftype DBType) func(DBValue, DBValue) bool {
	return func(t1, t2 DBValue) bool {
		switch ftype {
		case IntType:
			return evalPred(t1.(IntField).Value, t2.(IntField).Value, op)
		case StringType:
			return evalPred(t1.(StringField).Value, t2.(StringField).Value, op)
		}

		// Should never happen
		return false
	}
}

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	tuples := make([]*Tuple, 0)
	iter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	for {
		tuple, err := iter()
		if err != nil {
			return nil, err
		}

		if tuple == nil {
			break
		}

		tuples = append(tuples, tuple)
	}

	op := func(ascending bool) BoolOp {
		if ascending {
			return OpLe
		} else {
			return OpGt
		}
	}

	order_funcs := make([]lessFunc, 0)
	for i, expr := range o.orderBy {
		order_funcs = append(order_funcs, order_by_sub(op(o.ascending[i]), expr.GetExprType().Ftype))
	}

	OrderedBy(order_funcs, o.orderBy).Sort(tuples)
	idx := 0

	return func() (*Tuple, error) {
		if idx >= len(tuples) {
			return nil, nil
		}

		idx++
		return tuples[idx-1], nil
	}, nil
}
