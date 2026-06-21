package repo

import (
	"math/rand"
	"time"

	"github.com/jmoiron/sqlx"
)

func getDBConnString() string {

	return "host=localhost port=5432 user=user password=password dbname=events sslmode=disable"
}

func NewDBDonnection() (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", getDBConnString())
	if err != nil {
		return nil, err
	}
	return db, nil
}

var seedRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

var charSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHUIIJKLMNOPQRSTUVWXYZ0123456789"

func GenerateRandomStirng(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charSet[seedRand.Intn(len(charSet))]
	}
	return string(b)
}
