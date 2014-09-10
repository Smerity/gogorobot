package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	httpclient "github.com/mreiferson/go-httpclient"
	"github.com/op/go-logging"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var log = logging.MustGetLogger("gogorobot")
var format = "%{color}%{time:15:04:05.000000} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}"

type RobotResponse struct {
	Domain    string
	Url       string
	HasRobots bool
	FetchTime time.Time
	Body      []byte
}

func fetchRobot(fetchPipeline chan string, savePipeline chan RobotResponse, fetchGroup *sync.WaitGroup) {
	defer fetchGroup.Done()
	// Set timeout details for HTTP GET requests
	transport := &httpclient.Transport{
		ConnectTimeout:        10 * time.Second,
		RequestTimeout:        30 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	}
	defer transport.Close()
	client := &http.Client{Transport: transport}
	//
	for domain := range fetchPipeline {
		log.Debug(fmt.Sprintf("FTCH: Fetching %s", domain))
		// RFC[3.1] states robots.txt must be accessible via HTTP
		url := "http://" + domain + "/robots.txt"
		req, _ := http.NewRequest("GET", url, nil)
		resp, err := client.Do(req)
		if err != nil {
			// Domain with no URL implies extreme badness
			savePipeline <- RobotResponse{domain, "", false, time.Now(), nil}
			continue
		}
		defer resp.Body.Close()
		// Work out what the final request URL is
		finalUrl := resp.Request.URL.String()
		// RFC[3.1] states 2xx should be considered success
		if resp.StatusCode < 200 || resp.StatusCode > 206 {
			savePipeline <- RobotResponse{domain, finalUrl, false, time.Now(), nil}
			continue
		}
		// RFC[3.1] states robots.txt should be text/plain
		// TODO: Handle silly sites like http://www.weibo.com/robots.txt => text/html
		for _, mtype := range resp.Header["Content-Type"] {
			if !strings.HasPrefix(mtype, "text/plain") {
				savePipeline <- RobotResponse{domain, finalUrl, false, time.Now(), nil}
				continue
			}
		}
		//
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		savePipeline <- RobotResponse{domain, finalUrl, true, time.Now(), body}
	}
	log.Debug(fmt.Sprintf("FTCH: Exiting %#v", fetchGroup))
}

func saveRobots(savePipeline chan RobotResponse, wg *sync.WaitGroup) {
	defer wg.Done()
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
		log.Debug("Creating robots table...")
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

	// Ticker -- commit once per second
	tick := time.Tick(1 * time.Second)
	saveCount := 0
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

Loop:
	for {
		select {
		case resp, channelOpen := <-savePipeline:
			if !channelOpen {
				break Loop
			}
			log.Debug(fmt.Sprintf("SV: Saving %s", resp.Domain))
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
			saveCount += 1
			log.Debug(fmt.Sprintf("SV: Saved %s", resp.Domain))
		case <-tick:
			log.Notice("Saving... %d in one second\n", saveCount)
			saveCount = 0
			// Reset counter and end old transaction
			err = tx.Commit()
			if err != nil {
				log.Fatal(err)
			}
			insertSql.Close()
			// Start new transaction
			tx, err = db.Begin()
			if err != nil {
				log.Fatal(err)
			}
			// Generate statement to insert entries
			insertSql, err = tx.Prepare(`insert into
				robots(domain, url, hasRobots, fetchTime, body)
				values(?, ?, ?, ?, ?)`)
			if err != nil {
				log.Fatal(err)
			}

		}
	}
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	insertSql.Close()
	// End transaction

	// Check how many we have
	rows, err = db.Query("SELECT COUNT(*) FROM robots")
	if err != nil {
		log.Fatal(err)
	}
	rows.Next()
	var total int
	rows.Scan(&total)
	log.Debug(fmt.Sprintf("Total URLs in DB: %d", total))
	rows.Close()
}

func main() {
	logging.SetFormatter(logging.MustStringFormatter(format))
	logging.SetLevel(logging.INFO, "gogorobot")
	//
	fetchPipeline := make(chan string)
	savePipeline := make(chan RobotResponse)
	// NOTE: Important to pass via pointer
	// Otherwise, a new WaitGroup is created
	var fetchGroup sync.WaitGroup
	var saveGroup sync.WaitGroup

	saveGroup.Add(1)
	go saveRobots(savePipeline, &saveGroup)

	for i := 0; i < 5000; i++ {
		fetchGroup.Add(1)
		go fetchRobot(fetchPipeline, savePipeline, &fetchGroup)
	}

	// Insert entries
	reader := bufio.NewReader(os.Stdin)
	for {
		domain, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		domain = strings.TrimRight(domain, "\r\n")
		log.Debug(fmt.Sprintf("MAIN: Providing %s to fetch pipeline", domain))
		fetchPipeline <- domain
	}
	close(fetchPipeline)
	//
	fetchGroup.Wait()
	close(savePipeline)
	saveGroup.Wait()
}
