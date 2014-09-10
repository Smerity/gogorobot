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

type RobotResponse struct {
	Domain    string
	Url       string
	HasRobots bool
	FetchTime time.Time
	Body      []byte
}

func fetchRobot(domain string, c chan RobotResponse) {
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
		c <- RobotResponse{domain, finalUrl, false, time.Now(), nil}
		return
	}
	// RFC[3.1] states robots.txt should be text/plain
	// TODO: Handle silly sites like http://www.weibo.com/robots.txt => text/html
	for _, mtype := range resp.Header["Content-Type"] {
		if !strings.HasPrefix(mtype, "text/plain") {
			c <- RobotResponse{domain, finalUrl, false, time.Now(), nil}
			return
		}
	}
	//
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	c <- RobotResponse{domain, finalUrl, true, time.Now(), body}
	return
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
			fetchTime TIMESTAMP,
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
		robots(domain, url, hasRobots, fetchTime, body)
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

		c := make(chan RobotResponse)
		go fetchRobot(domain, c)
		resp := <-c

		_, err = insertSql.Exec(
			resp.Domain,
			resp.Url,
			resp.HasRobots,
			resp.FetchTime,
			string(resp.Body),
		)
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
