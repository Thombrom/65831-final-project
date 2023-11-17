package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// HeapFile is an unordered collection of tuples Internally, it is arranged as a
// set of heapPage objects
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// TODO: some code goes here
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	mutex   sync.Mutex
	bufPool *BufferPool
	sync.Mutex
	file *os.File
	desc *TupleDesc

	// Reports whether the file is continuous. If so,
	// when inserting a tuple, we can skip all but the
	// last page to check to insert
	fileContinuous bool
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	file, err := os.OpenFile(fromFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	return &HeapFile{bufPool: bp, file: file, desc: td, fileContinuous: false}, nil //replace me
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	info, err := f.file.Stat()
	if err != nil {
		return 0
	}

	// We ensure that every page is exactly PageSize number of bytes
	size := info.Size()
	return int(size) / PageSize
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for j := 0; j < f.NumPages(); j++ {
			pg, err := bp.GetPage(f, j, tid, ReadPerm)
			if pg == nil || err != nil {
				fmt.Println("page nil or error", err)
				break
			}
			if (*pg).isDirty() {
				(*f).flushPage(pg)
				(*pg).setDirty(false)
			}

		}

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [heapPage] object, using
// the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (*Page, error) {
	if pageNo >= f.NumPages() {
		return nil, GoDBError{TupleNotFoundError, "PageNo out of range"}
	}

	buf := make([]byte, PageSize)
	_, err := f.file.ReadAt(buf, int64(pageNo*PageSize))
	if err != nil {
		return nil, err
	}

	bbuf := bytes.NewBuffer(buf)
	page := newHeapPage(f.desc, pageNo, f)
	err = page.initFromBuffer(bbuf)
	if err != nil {
		return nil, err
	}

	// Convert
	var ppage Page
	ppage = page

	return &ppage, nil
}

// Add the tuple to the HeapFile.  This method should search through pages in
// the heap file, looking for empty slots and adding the tuple in the first
// empty slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile.  We will
// add support for concurrent modifications in lab 3.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	startPageno := 0
	if f.fileContinuous {
		startPageno = f.NumPages() - 1
	}

	for pageNo := startPageno; pageNo < f.NumPages(); pageNo++ {

		page, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
		if err != nil {
			return err
		}

		heap_page := (*page).(*heapPage)
		_, err = heap_page.insertTuple(t)
		if err != nil {
			continue
		}

		// We have inserted into the page so we set dirty
		return nil
	}

	// Create a new page and flush it
	new_pageno := f.NumPages()
	heap_page := newHeapPage(f.desc, new_pageno, f)

	var ppage Page
	ppage = heap_page
	err := f.flushPage(&ppage)
	if err != nil {
		return err
	}

	page, err := f.bufPool.GetPage(f, new_pageno, tid, ReadPerm)
	if err != nil {
		return err
	}

	heap_page = (*page).(*heapPage)
	_, err = heap_page.insertTuple(t)
	if err != nil {
		return err
	}

	// The file must be continuous since we have to make a new page
	f.fileContinuous = true
	return nil
}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	rid := t.Rid.(RId)

	if rid.pageNo < 0 || rid.pageNo >= f.NumPages() {
		return GoDBError{TupleNotFoundError, fmt.Sprintf("PageNo %d is out of range", rid.pageNo)}
	}

	page, err := f.bufPool.GetPage(f, rid.pageNo, tid, WritePerm)
	if err != nil {
		return err
	}

	heap_page := (*page).(*heapPage)
	err = heap_page.deleteTuple(rid)
	if err != nil {
		return err
	}

	return nil
}

// Method to force the specified page back to the backing file at the appropriate
// location.  This will be called by BufferPool when it wants to evict a page.
// The Page object should store information about its offset on disk (e.g.,
// that it is the ith page in the heap file), so you can determine where to write it
// back.
func (f *HeapFile) flushPage(p *Page) error {
	heap_page := (*p).(*heapPage)
	buf, err := heap_page.toBuffer()
	if err != nil {
		return err
	}

	f.file.WriteAt(buf.Bytes(), int64(heap_page.pageNo)*int64(PageSize))
	return nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	return f.desc
}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	pageNo := -1
	itt := func() (*Tuple, error) {
		return nil, nil
	}

	var exhaust func() (*Tuple, error)

	exhaust = func() (*Tuple, error) {
		t, e := itt()
		if t != nil {
			return t, e
		}

		pageNo = pageNo + 1
		if pageNo < f.NumPages() {
			page, err := f.bufPool.GetPage(f, pageNo, tid, ReadPerm)
			if err != nil {
				return nil, err
			}

			heap_page := (*page).(*heapPage)
			itt = heap_page.tupleIter()
		} else {
			return nil, nil
		}

		return exhaust()
	}

	return exhaust, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	info, _ := f.file.Stat()
	return heapHash{FileName: info.Name(), PageNo: pgNo}
}
