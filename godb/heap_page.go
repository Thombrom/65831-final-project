package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	desc   *TupleDesc
	tuples []*Tuple
	dirty  bool
	pageNo int
	file   *HeapFile
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	// Calculate the number of slots we have
	bytesForSlots := PageSize - 8
	bytesPerTuple := desc.BinarySize()

	numSlots := bytesForSlots / bytesPerTuple
	return &heapPage{tuples: make([]*Tuple, numSlots), desc: desc, dirty: false, pageNo: pageNo, file: f}
}

func (h *heapPage) getNumSlots() int {
	bytesForSlots := PageSize - 8
	bytesPerTuple := h.desc.BinarySize()

	return bytesForSlots / bytesPerTuple
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	for i, tuple := range h.tuples {
		if tuple == nil {
			t.Rid = RId{pageNo: h.pageNo, slotNo: i}
			h.tuples[i] = t
			h.setDirty(true)
			return t.Rid, nil
		}
	}

	return 0, GoDBError{PageFullError, fmt.Sprintf("Could not insert tuple, page already full")}
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	rid_, ok := rid.(RId)
	if !ok {
		return GoDBError{TypeMismatchError, "Could not convert recordID into RId"}
	}

	if rid_.pageNo != h.pageNo {
		return GoDBError{TupleNotFoundError, fmt.Sprintf("Record id not found. Pageno mismatch (%d != %d)", h.pageNo, rid_.pageNo)}
	}

	if rid_.slotNo >= h.getNumSlots() || rid_.slotNo < 0 {
		return GoDBError{TupleNotFoundError, fmt.Sprintf("Record id not found. Slotno out of bound (%d not in [%d, %d))", rid_.slotNo, 0, h.getNumSlots())}
	}

	if h.tuples[rid_.slotNo] == nil {
		return GoDBError{TupleNotFoundError, fmt.Sprintf("No tuple exists at slotno %d", rid_.slotNo)}
	}

	h.tuples[rid_.slotNo] = nil
	h.setDirty(true)
	return nil

}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (h *heapPage) getFile() *DBFile {
	var dbfile DBFile
	dbfile = h.file
	return &dbfile
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)

	var used int32 = 0
	for _, tuple := range h.tuples {
		if tuple != nil {
			used += 1
		}
	}

	slots := int32(h.getNumSlots())
	binary.Write(buf, binary.LittleEndian, &slots)
	binary.Write(buf, binary.LittleEndian, &used)

	for _, tuple := range h.tuples {
		if tuple != nil {
			err := tuple.writeTo(buf)
			if err != nil {
				return buf, err
			}
		}
	}

	// Make sure that pages serialize to exactly PageSize number of bytes
	padding := make([]byte, PageSize-int(used)*h.desc.BinarySize()-8)
	binary.Write(buf, binary.LittleEndian, padding)

	return buf, nil

}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	var slots int32
	var used int32

	binary.Read(buf, binary.LittleEndian, &slots)
	binary.Read(buf, binary.LittleEndian, &used)
	h.tuples = make([]*Tuple, slots)

	for i := 0; i < int(used); i++ {
		val, err := readTupleFrom(buf, h.desc)
		if err != nil {
			return err
		}

		val.Rid = RId{pageNo: h.pageNo, slotNo: i}
		h.tuples[i] = val
	}

	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	idx := 0

	return func() (*Tuple, error) {
		for idx < p.getNumSlots() {
			idx += 1

			if p.tuples[idx-1] != nil {
				return p.tuples[idx-1], nil
			}
		}

		return nil, nil
	}
}
