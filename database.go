package main

import (
	"github.com/tidwall/btree"
	"sync"
)

type Value struct {
	txnStartId uint64
	txnEndId   uint64
	value      string
}

type TransactionState uint8

const (
	InProgressTransactionState TransactionState = iota
	CommittedTransactionState
	AbortedTransactionState
)

// IsolationLevel Isolation level strictness increases downwards
type IsolationLevel uint8

const (
	ReadUncommittedIsolationLevel IsolationLevel = iota
	ReadCommittedIsolationLevel
	ReadRepeatableIsolationLevel
	SnapshotIsolationLevel
	SerializableIsolationLevel
)

// Transaction models a transaction that occurs in a database.
type Transaction struct {
	ID    uint64 // monotic increasing integer
	Level IsolationLevel
	State TransactionState

	InProgress btree.Set[uint64]

	// these fields are used for snapshot isolation and stricter ones (serializable)
	WriteSet btree.Set[string]
	ReadSet  btree.Set[string]
}

type Database struct {
	// Except changed all transactions will have the same isolation level.
	DefaultIsolationLevel IsolationLevel
	Store                 map[uint64][]Value
	Transactions          btree.Map[uint64, *Transaction]
	State                 TransactionState
	NextTransactionID     uint64

	// locks to control
	mu sync.Mutex
}

func NewDatabase() *Database {
	return &Database{
		DefaultIsolationLevel: ReadCommittedIsolationLevel,
		Store:                 map[uint64][]Value{},
		NextTransactionID:     1,
		mu:                    sync.Mutex{},
	}
}

func (db *Database) NewTransaction() *Transaction {
	db.mu.Lock()
	defer db.mu.Unlock()
	txn := &Transaction{
		ID:    db.NextTransactionID,
		State: InProgressTransactionState,
		Level: db.DefaultIsolationLevel,
	}
	db.NextTransactionID++ // increase transactionID everytime it is created.

	// make this new transaction aware of other transactions in progress
	txn.InProgress = db.getTransactionsInProgress()

	// store this transaction in the db
	db.Transactions.Set(txn.ID, txn)

	debug("New transaction created with id:", txn.ID)

	return txn
}

func (db *Database) UpdateTransaction(txn *Transaction, value TransactionState) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	debug("updating transaction state to:", value, "from:", txn.State, "for txn:", txn.ID)
	txn.State = value
	db.Transactions.Set(txn.ID, txn)
	debug("updated transaction to:", txn.State, "for txn:", txn.ID)

	return nil
}

func (db *Database) GetTransactionState(id uint64) *Transaction {
	db.mu.Lock()
	defer db.mu.Unlock()

	txn, ok := db.Transactions.Get(id)
	assert(ok, "valid transaction fetched")

	return txn
}

func (db *Database) assertValidTransaction(txn *Transaction) {
	assert(txn.ID > 0, "transaction is valid. id > 0")
	assert(db.GetTransactionState(txn.ID).State == InProgressTransactionState, "in progress transaction")
}

// getTransactionsInProgress fetches all transaction ids that are still in progress
//
// Returns
// - btree.Set[uint64]: a set of ids of transactions in progress
func (db *Database) getTransactionsInProgress() (ids btree.Set[uint64]) {
	iter := db.Transactions.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		// check if transaction is in progress
		if txn := iter.Value(); txn.State == InProgressTransactionState {
			ids.Insert(txn.ID)
		}
	}

	return ids
}
