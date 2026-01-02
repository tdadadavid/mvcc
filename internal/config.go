package internal

import (
	"os"
)

type Config struct {
	DBURL string
}

func LoadConfig() *Config {
	dbURL := os.Getenv("DB_URL")

	return &Config{
		DBURL: dbURL,
	}
}
