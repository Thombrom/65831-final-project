package godb

import "golang.org/x/exp/constraints"

type Number interface {
	constraints.Integer | constraints.Float
}

// interface for an aggregation state
type AggState interface {

	// Initializes an aggregation state. Is supplied with an alias,
	// an expr to evaluate an input tuple into a DBValue, and a getter
	// to extract from the DBValue its int or string field's value.
	Init(alias string, expr Expr, getter func(DBValue) any) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func intAggGetter(v DBValue) any {
	return v.(IntField).Value
}

func stringAggGetter(v DBValue) any {
	return v.(StringField).Value
}

// Implements the aggregation state for SUM
type SumAggState[T Number] struct {
	alias string
	expr  Expr
	aggr  T
}

func (a *SumAggState[T]) Copy() AggState {
	return &SumAggState[T]{a.alias, a.expr, a.aggr}
}

func (a *SumAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.expr = expr
	a.alias = alias
	a.aggr = 0
	return nil // TODO change me
}

func (a *SumAggState[T]) AddTuple(t *Tuple) {
	value, _ := a.expr.EvalExpr(t)
	a.aggr += T(value.(IntField).Value)
}

func (a *SumAggState[T]) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *SumAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.aggr)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState[T Number] struct {
	alias string
	expr  Expr
	sum   T
	count int
}

func (a *AvgAggState[T]) Copy() AggState {
	return &AvgAggState[T]{alias: a.alias, expr: a.expr, sum: a.sum, count: a.count}
}

func (a *AvgAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.alias = alias
	a.expr = expr
	a.sum = 0
	a.count = 0
	return nil
}

func (a *AvgAggState[T]) AddTuple(t *Tuple) {
	value, _ := a.expr.EvalExpr(t)
	a.sum += T(value.(IntField).Value)
	a.count += 1
}

func (a *AvgAggState[T]) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *AvgAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(int(a.sum) / a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState[T constraints.Ordered] struct {
	alias          string
	getter         func(DBValue) any
	max            DBValue
	expr           Expr
	seen_value_yet bool
}

func (a *MaxAggState[T]) Copy() AggState {
	return &MaxAggState[T]{a.alias, a.getter, a.max, a.expr, a.seen_value_yet}
}

func (a *MaxAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.alias = alias
	a.expr = expr
	a.getter = getter
	a.seen_value_yet = false
	return nil
}

func (a *MaxAggState[T]) AddTuple(t *Tuple) {
	value, _ := a.expr.EvalExpr(t)
	v := a.getter(value)

	if !a.seen_value_yet {
		a.max = v
		return
	}

	if a.expr.GetExprType().Ftype == IntType {
		if v.(int64) > a.max.(int64) {
			a.max = v
		}
	} else {
		if v.(string) > a.max.(string) {
			a.max = v
		}
	}
}

func (a *MaxAggState[T]) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", a.expr.GetExprType().Ftype}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MaxAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	if a.expr.GetExprType().Ftype == IntType {
		f := IntField{a.max.(int64)}
		t := Tuple{*td, []DBValue{f}, nil}
		return &t
	} else {
		f := StringField{a.max.(string)}
		t := Tuple{*td, []DBValue{f}, nil}
		return &t
	}
}

// Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState[T constraints.Ordered] struct {
	alias          string
	getter         func(DBValue) any
	min            DBValue
	expr           Expr
	seen_value_yet bool
}

func (a *MinAggState[T]) Copy() AggState {
	return &MinAggState[T]{a.alias, a.getter, a.min, a.expr, a.seen_value_yet}
}

func (a *MinAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.alias = alias
	a.expr = expr
	a.getter = getter
	a.seen_value_yet = false
	return nil
}

func (a *MinAggState[T]) AddTuple(t *Tuple) {
	value, _ := a.expr.EvalExpr(t)
	v := a.getter(value)

	if !a.seen_value_yet {
		a.min = v
		return
	}

	if a.expr.GetExprType().Ftype == IntType {
		if v.(int64) > a.min.(int64) {
			a.min = v
		}
	} else {
		if v.(string) > a.min.(string) {
			a.min = v
		}
	}
}

func (a *MinAggState[T]) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", a.expr.GetExprType().Ftype}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MinAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	if a.expr.GetExprType().Ftype == IntType {
		f := IntField{a.min.(int64)}
		t := Tuple{*td, []DBValue{f}, nil}
		return &t
	} else {
		f := StringField{a.min.(string)}
		t := Tuple{*td, []DBValue{f}, nil}
		return &t
	}
}
