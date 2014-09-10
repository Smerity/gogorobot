package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"time"
)

func main() {
	// Open database
	db, err := sql.Open("sqlite3", "./robots.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create tables
	createSql := `CREATE TABLE robots(
		id INTEGER NOT NULL PRIMARY KEY,
		url TEXT,
		fetched TIMESTAMP,
		body TEXT
		)`

	_, err = db.Exec(createSql)
	if err != nil {
		log.Printf("%q: %s\n", err, createSql)
		return
	}

	// Generate statement to insert entries
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	insertSql, err := tx.Prepare(`insert into
		robots(url, fetched, body)
		values(?, ?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	defer insertSql.Close()

	// Insert entries
	reader := bufio.NewReader(os.Stdin)
	for {
		url, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		log.Println("Fetching " + url)

		_, err = insertSql.Exec(url, time.Now(), "robots")
		if err != nil {
			log.Fatal(err)
		}
	}
	tx.Commit()

	// Check how many we have
	rows, err := db.Query("SELECT COUNT(*) FROM robots")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var total string
		rows.Scan(&total)
		fmt.Println(total)
	}
}
