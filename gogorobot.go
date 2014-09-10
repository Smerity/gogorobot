package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func fetchRobot(domain string) (bool, string, []byte) {
	// RFC[3.1] states robots.txt must be accessible via HTTP
	url := "http://" + domain + "/robots.txt"
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	// Work out what the final request URL is
	finalUrl := resp.Request.URL.String()
	// RFC[3.1] states 2xx should be considered success
	if resp.StatusCode < 200 || resp.StatusCode > 206 {
		return false, finalUrl, nil
	}
	// RFC[3.1] states robots.txt should be text/plain
	// TODO: Handle silly sites like http://www.weibo.com/robots.txt => text/html
	for _, mtype := range resp.Header["Content-Type"] {
		if !strings.HasPrefix(mtype, "text/plain") {
			return false, finalUrl, nil
		}
	}
	//
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	return true, finalUrl, body
}

func main() {
	// Open database
	db, err := sql.Open("sqlite3", "./robots.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create tables if needed
	rows, err := db.Query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='robots'")
	if err != nil {
		log.Fatal(err)
	}
	rows.Next()
	var tableCreated bool
	rows.Scan(&tableCreated)
	//
	if !tableCreated {
		log.Println("Creating robots table...")
		createSql := `CREATE TABLE robots(
			id INTEGER NOT NULL PRIMARY KEY,
			domain TEXT,
			url TEXT,
			hasRobots INT,
			fetched TIMESTAMP,
			body TEXT
			)`
		_, err = db.Exec(createSql)
		if err != nil {
			log.Fatal(err)
		}
	}
	rows.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	// Generate statement to insert entries
	insertSql, err := tx.Prepare(`insert into
		robots(domain, url, hasRobots, fetched, body)
		values(?, ?, ?, ?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	defer insertSql.Close()

	// Insert entries
	reader := bufio.NewReader(os.Stdin)
	for {
		domain, err := reader.ReadString('\n')
		domain = strings.TrimRight(domain, "\r\n")
		if err != nil {
			break
		}
		log.Println("Fetching " + domain)

		success, url, body := fetchRobot(domain)

		_, err = insertSql.Exec(domain, url, success, time.Now(), string(body))
		if err != nil {
			log.Fatal(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	// End transaction

	// Check how many we have
	rows, err = db.Query("SELECT COUNT(*) FROM robots")
	if err != nil {
		log.Fatal(err)
	}
	rows.Next()
	var total string
	rows.Scan(&total)
	fmt.Println(total)
	rows.Close()
}
