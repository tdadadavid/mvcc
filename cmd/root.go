package cmd

import (
	"fmt"
	"os"
	"pg-mvcc/internal"
)

func Execute() {
	config := internal.LoadConfig()
	fmt.Println("DB_URL:", config.DBURL)
	os.Exit(0)
}
