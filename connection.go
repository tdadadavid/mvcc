package main

import "fmt"

// Connection models a client connection to the database
type Connection struct {
	db  *Database
	txn *Transaction // a connection contains at most one transaction
}

func (c *Connection) execCommand(command string, args []string) (string, error) {
	debug(command, args)

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
