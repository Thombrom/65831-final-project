package godb

import (
	"fmt"
	"sync"
	"time"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

type LockType int

const (
	ReadLock  LockType = iota
	WriteLock LockType = iota
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	mutex              sync.Mutex
	log                *Log
	filenames_to_files map[string]*HeapFile

	locks    map[heapHash]map[TransactionID]LockType
	waitfor  map[TransactionID]map[TransactionID]bool // A transaction can wait for multiple others (think acquire write lock when multiple has read locks)
	pages    map[heapHash]*Page
	numPages int
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int, log_fromfile string) *BufferPool {
	var log *Log

	if len(log_fromfile) != 0 {
		log, _ = newLog(log_fromfile)
	}

	return &BufferPool{
		log:                log,
		filenames_to_files: make(map[string]*HeapFile),
		pages:              make(map[heapHash]*Page, 0),
		locks:              make(map[heapHash]map[TransactionID]LockType, 0),
		numPages:           numPages,
		waitfor:            make(map[TransactionID]map[TransactionID]bool, 0),
	}
}

func (bp *BufferPool) appendLog(record *LogRecord) error {
	if bp.log != nil {
		return bp.log.Append(record)
	}

	return nil
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	for hh, page := range bp.pages {
		dbfile := (*page).getFile()
		(*dbfile).flushPage(page)
		delete(bp.pages, hh)
	}
}

func (bp *BufferPool) UndoTransaction(tid TransactionID) error {
	reader, err := bp.log.CreateLogReaderAtEnd()
	if err != nil {
		return err
	}

	for !reader.AtStart() {
		err = reader.AdvancePrev()
		if err != nil {
			return err
		}

		metadata, err := reader.ReadMetadata()
		if err != nil {
			return err
		}

		if *metadata.tid != *tid {
			continue
		}

		if metadata.optype == LogBeginTransaction {
			break
		}

		if metadata.optype != LogInsertDelete {
			continue
		}

		file := bp.filenames_to_files[metadata.hh.FileName]
		record, err := reader.ReadLogRecord(file.desc)

		// We obviously hold the lock on this page since we were able to edit it
		// so it should be no problem to edit it here
		page, ok := bp.pages[record.metadata.hh]
		if ok {
			(*page).(*heapPage).insertTupleAt(record.undo.tuple, int(record.undo.pd.SlotNo))
		} else {
			page, err = file.readPage(record.metadata.hh.PageNo)
			if err != nil {
				return err
			}
			(*page).(*heapPage).insertTupleAt(record.undo.tuple, int(record.redo.pd.SlotNo))
			err = file.flushPage(page)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (bp *BufferPool) FinishTransaction(tid TransactionID, commit bool) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	keys_to_release := make([]heapHash, 0)
	for key, page_locks := range bp.locks {
		page_tids_to_release := make([]TransactionID, 0)

		for page_tid := range page_locks {
			if page_tid == tid {
				page_tids_to_release = append(page_tids_to_release, page_tid)

				// If we commit, we flush the page,
				// otherwise we delete it from the bp
				if bp.log == nil {
					if commit {
						page, ok := bp.pages[key]
						if !ok {
							continue
						}

						// Flush page. We can now mark it as non-dirty
						dbfile := (*page).getFile()
						(*dbfile).flushPage(page)
						(*page).setDirty(false)
					} else {
						delete(bp.pages, key)
					}
				} else {
					// If aborting, we need to go back and undo the transaction
					if !commit {
						err := bp.UndoTransaction(tid)
						if err != nil {
							fmt.Println("!!! Error in undoing transaction !!!")
						}
					}
				}

				if len(page_locks) == 1 {
					keys_to_release = append(keys_to_release, key)
				}
			}
		}

		for _, page_tid := range page_tids_to_release {
			delete(page_locks, page_tid)
		}
	}

	for _, key := range keys_to_release {
		delete(bp.locks, key)
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	bp.appendLog(AbortTransactionLog(tid))
	bp.FinishTransaction(tid, false)
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	bp.appendLog(CommitTransactionLog(tid))
	bp.FinishTransaction(tid, true)
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	bp.appendLog(BeginTransactionLog(tid))
	return nil
}

func (bp *BufferPool) EvictPageL() error {
	for k, v := range bp.pages {
		if !((*v).isDirty()) {
			delete(bp.pages, k)
			return nil
		}
	}

	if bp.log != nil {

		// We have a buffer pool full of dirty pages, so we delete the first page we come across
		// Note that this is only possible because we have the log so we can go back and logically
		// undo the operations
		for k, page := range bp.pages {
			file := (*page).(*heapPage).getFile()
			(*file).(*HeapFile).flushPage(page)
			delete(bp.pages, k)
			return nil
		}
	}

	return GoDBError{BufferPoolFullError, fmt.Sprintf("Buffer pool full of dirty pages - cannot evict page")}
}

// Assumes there is a
func HighestLockHeld(locks map[TransactionID]LockType) LockType {
	for _, v := range locks {
		if v == WriteLock {
			return WriteLock
		}
	}

	return ReadLock
}

func PermToLocktype(perm RWPerm) LockType {
	if perm == ReadPerm {
		return ReadLock
	}
	return WriteLock
}

func (bp *BufferPool) DetectDeadlockImplL(tid TransactionID, target TransactionID, visited *map[TransactionID]bool) error {
	if tid == target {
		return GoDBError{DeadlockError, fmt.Sprintf("Deadlock detected")}
	}

	(*visited)[tid] = true

	// If the other transaction is not waiting for anything,
	// there is no way this could be a deadlock
	waitfor, ok := bp.waitfor[tid]
	if !ok {
		return nil
	}

	for waittid := range waitfor {
		_, ok := (*visited)[waittid]

		// There is some cycle, but we're not part of this cycle
		if ok {
			return nil
		}

		err := bp.DetectDeadlockImplL(waittid, target, visited)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bp *BufferPool) DetectDeadlockL(tid TransactionID) error {
	waitfor := bp.waitfor[tid]
	visited := make(map[TransactionID]bool, 0)
	for waittid := range waitfor {
		err := bp.DetectDeadlockImplL(waittid, tid, &visited)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bp *BufferPool) WaitL() {
	bp.mutex.Unlock()
	time.Sleep(5 * time.Millisecond)
	bp.mutex.Lock()
}

// Blocks until we acquire the lock.
func (bp *BufferPool) AcquireLockL(key heapHash, tid TransactionID, perm RWPerm) error {
	// When we exit AcquireLock we must necessarily hold the lock or be aborting
	defer func() { delete(bp.waitfor, tid) }()

start:
	locks, ok := bp.locks[key]
	if !ok {
		// No locks on this key, so we can allocate the map and take the lock
		// right away
		locks = make(map[TransactionID]LockType, 0)
		bp.locks[key] = locks
		locks[tid] = PermToLocktype(perm)
		return nil
	}

	// There are currently somebody who has a lock on the key
	lockcount := len(locks)
	highest_lock := HighestLockHeld(locks)

	_, ok = locks[tid]
	if ok && lockcount == 1 {
		// We have a lock already, and we're the only ones with a lock,
		// which means we're free to upgrade it to whatever.

		// If we already hold a writelock, we're good
		if highest_lock == WriteLock {
			return nil
		}

		// We hold a readlock, so we need to either stick to the
		// readlock or upgrade it
		locks[tid] = PermToLocktype(perm)
		return nil
	}

	if ok && lockcount > 1 {
		// We have a lock, but someone else also has
		// a lock. We can proceed if we're trying to
		// acquire a readlock and what they have is a
		// readlock, otherwise we have to wait

		if PermToLocktype(perm) == ReadLock && highest_lock == ReadLock {
			locks[tid] = ReadLock
			return nil
		}

		// Wait for a bit and then start over. Save who we wait for
		waitfor := make(map[TransactionID]bool, 0)
		for k := range locks {
			waitfor[k] = true
		}
		bp.waitfor[tid] = waitfor
		err := bp.DetectDeadlockL(tid)
		if err != nil {
			bp.mutex.Unlock()
			bp.AbortTransaction(tid)
			bp.mutex.Lock()
			return err
		}

		bp.WaitL()
		goto start
	}

	// The zero case is handled by the invariant that an entry does
	// not exist if there are no locks held
	if !ok && lockcount > 0 {
		if PermToLocktype(perm) == ReadLock && highest_lock == ReadLock {
			locks[tid] = ReadLock
			return nil
		}

		// Create dependency graph to check for deadlock
		waitfor := make(map[TransactionID]bool, 0)
		for k := range locks {
			waitfor[k] = true
		}
		bp.waitfor[tid] = waitfor
		err := bp.DetectDeadlockL(tid)
		if err != nil {
			bp.mutex.Unlock()
			bp.AbortTransaction(tid)
			bp.mutex.Lock()
			return err
		}

		bp.WaitL()
		goto start
	}

	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	pagekey := file.pageKey(pageNo).(heapHash)
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	bp.filenames_to_files[pagekey.FileName] = (file).(*HeapFile)

	err := bp.AcquireLockL(pagekey, tid, perm)
	if err != nil {
		return nil, err
	}

	_, ok := bp.pages[pagekey]
	if !ok {
		// Check if we have to evict a page
		if len(bp.pages) >= bp.numPages {
			err := bp.EvictPageL()
			if err != nil {
				return nil, err
			}
		}

		// Load page
		page, err := file.readPage(pageNo)
		if err != nil {
			return nil, err
		}

		bp.pages[pagekey] = page
	}

	return bp.pages[pagekey], nil
}
