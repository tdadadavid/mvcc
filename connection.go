package main

import "fmt"

type Command string

func (c Command) String() string { return string(c) }

var (
	BEGIN  Command = "BEGIN"
	ABORT  Command = "ABORT"
	COMMIT Command = "COMMIT"
	GET    Command = "GET"
	SET    Command = "SET"
	DELETE Command = "DELETE"
)

// Connection models a client connection to the database
type Connection struct {
	db  *Database
	txn *Transaction // a connection contains at most one transaction
}

func (c *Connection) execCommand(command string, args []string) (string, error) {
	debug(command, args)

	// if a user tries to start a new transaction
	if command == BEGIN.String() {
		// check if this connection already has an ongoing transaction
		assertEq(c.txn, nil, "connection has no transaction")
		c.txn = c.db.NewTransaction()      // create a new transaction for the user connection
		c.db.assertValidTransaction(c.txn) // check to ensure the transaction is valid
		return fmt.Sprintf("%d", c.txn.ID), nil
	}

	if command == ABORT.String() {
		c.db.assertValidTransaction(c.txn)
		err := c.db.CompleteTransaction(c.txn, AbortedTransactionState) // mark transaction as aborted
		c.txn = nil                                                     // remove transaction from the client connection
		return "", err
	}

	if command == COMMIT.String() {
		c.db.assertValidTransaction(c.txn)
		err := c.db.CompleteTransaction(c.txn, CommittedTransactionState) // mark the transaction as commited
		c.txn = nil                                                       // release transaction from the connection
		return "", err
	}

	if command == GET.String() {
		c.db.assertValidTransaction(c.txn)

		key := args[0]

		c.txn.ReadSet.Insert(key) // track the key in the read set (used by snapshot and serializable isolation level)

		// check if the key exists in a database
		values := c.db.Store[key]
		if values == nil {
			return "", fmt.Errorf("key %s not found", key)
		}

		for i := len(values) - 1; i >= 0; i-- {
			value := values[i]
			debug(value, c.txn, "isVisible=", c.db.IsVisible(c.txn, &value))
			if c.db.IsVisible(c.txn, &value) {
				return value.value, nil
			}
		}

	}

	if command == SET.String() || command == DELETE.String() {
		c.db.assertValidTransaction(c.txn) // validate the transaction within this connection is valid

		key := args[0]
		input := args[1]

		values := c.db.Store[key]
		if values == nil {
			return "", fmt.Errorf("key %s not found", key)
		}

		// mark all values visible to this transaction invalid (stale)
		found := false
		for i := len(values) - 1; i >= 0; i-- {
			value := values[i]
			if c.db.IsVisible(c.txn, &value) {
				// mark the value as stale by setting the transaction end id, meaning that this particular
				// value became stale during this transaction process
				value.txnEndId = c.txn.ID
				found = true
			}
		}

	}

	return "", nil
}

func (c *Connection) mustExecCommand(command string, args []string) string {
	result, err := c.execCommand(command, args)
	assertEq(err, nil, fmt.Sprintf("unexpected error command=%s", command))
	return result
}

func NewConnection(db *Database) *Connection {
	return &Connection{
		db:  db,
		txn: nil,
	}
}
