package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

var typeNames map[DBType]string = map[DBType]string{IntType: "int", StringType: "string"}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

func field_type_equal(f1 *FieldType, f2 *FieldType) bool {
	return f1.Fname == f2.Fname &&
		f1.TableQualifier == f2.TableQualifier &&
		f1.Ftype == f2.Ftype
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Returns the number of bytes used when serializing a
// tuple of this kind
func (t *TupleDesc) BinarySize() int {
	bytesUsed := 0

	for _, field := range t.Fields {
		switch field.Ftype {
		case IntType:
			bytesUsed += 8
		case StringType:
			bytesUsed += StringLength
		}
	}

	return bytesUsed
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	// If they are not the same length they cannot be the same
	if len(d1.Fields) != len(d2.Fields) {
		return false
	}

	for i := 0; i < len(d1.Fields); i++ {
		if !(field_type_equal(&d1.Fields[i], &d2.Fields[i])) {
			return false
		}
	}

	return true
}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
func (td *TupleDesc) copy() *TupleDesc {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	return &TupleDesc{fields}
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	fields := make([]FieldType, 0)

	for i := range desc.Fields {
		fields = append(fields, desc.Fields[i])
	}

	for i := range desc2.Fields {
		fields = append(fields, desc2.Fields[i])
	}

	return &TupleDesc{fields}
}

// ================== Tuple Methods ======================

// Interface used for tuple field values
// Since it implements no methods, any object can be used
// but having an interface for this improves code readability
// where tuple values are used
type DBValue interface {
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type RId struct {
	pageNo int
	slotNo int
}

type recordID interface{}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.

func write_int(value int64, b *bytes.Buffer) error {
	return binary.Write(b, binary.LittleEndian, value)
}

func write_string(value string, b *bytes.Buffer) error {
	bytes := []byte(value)
	err := binary.Write(b, binary.LittleEndian, bytes)

	padding := make([]byte, StringLength-len(bytes))
	err = binary.Write(b, binary.LittleEndian, padding)
	return err
}

func (t *Tuple) writeTo(b *bytes.Buffer) error {
	for i := range t.Fields {
		switch t.Desc.Fields[i].Ftype {
		case IntType:
			err := write_int(t.Fields[i].(IntField).Value, b)
			if err != nil {
				return err
			}
		case StringType:
			err := write_string(t.Fields[i].(StringField).Value, b)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func read_int(b *bytes.Buffer) (int64, error) {
	value := int64(0)
	err := binary.Read(b, binary.LittleEndian, &value)
	return value, err
}

func read_string(b *bytes.Buffer) (string, error) {
	value := make([]byte, StringLength)
	err := binary.Read(b, binary.LittleEndian, &value)

	value = bytes.TrimRight(value, "\u0000")
	return string(value), err
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	fields := make([]DBValue, 0)

	for _, field_descriptor := range desc.Fields {
		switch field_descriptor.Ftype {
		case IntType:
			value, err := read_int(b)
			if err != nil {
				return nil, err
			}

			fields = append(fields, IntField{value})
		case StringType:
			value, err := read_string(b)
			if err != nil {
				return nil, err
			}

			fields = append(fields, StringField{value})
		}
	}

	return &Tuple{Desc: *desc, Fields: fields, Rid: 0}, nil
}

func fields_equal(t1 *Tuple, t2 *Tuple) bool {
	for i := range t1.Fields {
		switch t1.Desc.Fields[i].Ftype {
		case IntType:
			if t1.Fields[i].(IntField).Value != t2.Fields[i].(IntField).Value {
				return false
			}

		case StringType:
			if t1.Fields[i].(StringField).Value != t2.Fields[i].(StringField).Value {
				return false
			}
		}
	}

	return true
}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	return t1.Desc.equals(&t2.Desc) && fields_equal(t1, t2)
}

// Merge two tuples together, producing a new tuple with the fields of t2 appended to t1.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	if t1 == nil {
		return t2
	}

	if t2 == nil {
		return t1
	}

	desc := t1.Desc.merge(&t2.Desc)

	fields := make([]DBValue, 0)
	fields = append(fields, t1.Fields...)
	fields = append(fields, t2.Fields...)

	return &Tuple{Desc: *desc, Fields: fields, Rid: 0}
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.

func compare_ints(v1 IntField, v2 IntField) orderByState {
	if v1.Value > v2.Value {
		return OrderedGreaterThan
	} else if v1.Value < v2.Value {
		return OrderedLessThan
	} else {
		return OrderedEqual
	}
}

func compare_strings(v1 StringField, v2 StringField) orderByState {
	if v1.Value > v2.Value {
		return OrderedGreaterThan
	} else if v1.Value < v2.Value {
		return OrderedLessThan
	} else {
		return OrderedEqual
	}
}

func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	v1, err := field.EvalExpr(t)
	if err != nil {
		return 0, err
	}

	v2, err := field.EvalExpr(t2)
	if err != nil {
		return 0, err
	}

	switch field.GetExprType().Ftype {
	case IntType:
		return compare_ints(v1.(IntField), v2.(IntField)), nil
	case StringType:
		return compare_strings(v1.(StringField), v2.(StringField)), nil
	}

	// Should never happen
	return 0, nil
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	desc := make([]FieldType, 0)
	projected_fields := make([]DBValue, 0)

	for _, field := range fields {
		idx, err := findFieldInTd(field, &t.Desc)
		if err == nil {
			desc = append(desc, t.Desc.Fields[idx])
			projected_fields = append(projected_fields, t.Fields[idx])
		}
	}

	return &Tuple{Desc: TupleDesc{Fields: desc}, Fields: projected_fields, Rid: 0}, nil
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {

	//todo efficiency here is poor - hashstructure is probably slow
	hash, _ := hashstructure.Hash(t, hashstructure.FormatV2, nil)

	return hash
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = fmt.Sprintf("%d", f.Value)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr

}
