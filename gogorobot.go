package main

// Special notes re: DNS resolution
// https://code.google.com/p/go/issues/detail?id=3575
// https://groups.google.com/forum/#!topic/golang-nuts/pP3zyUlbT00
// http://grokbase.com/t/gg/golang-nuts/142vch7a3t/go-nuts-tcp-dial-dns-lookup-errors
// https://groups.google.com/forum/#!topic/golang-nuts/wliZf2_LUag
// https://code.google.com/p/go/issues/detail?id=8434
// nasa.gov & navy.mil fail as they require www

import (
	"bufio"
	_ "crypto/sha512" // See http://bridge.grumpy-troll.org/2014/05/golang-tls-comodo/
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	httpclient "github.com/mreiferson/go-httpclient"
	"github.com/op/go-logging"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

var log = logging.MustGetLogger("gogorobot")
var format = "%{color}%{time:15:04:05.000000} ▶ %{level:.4s} %{id:03x}%{color:reset} %{shortfile} %{message}"

type FetchRequest struct {
	Domain  string
	Attempt uint
}

type RobotResponse struct {
	Domain    string
	Url       string
	HasRobots bool
	FetchTime time.Time
	Body      []byte
	Redirects int
}

func fetchRobot(fetchPipeline chan FetchRequest, savePipeline chan RobotResponse, fetchGroup *sync.WaitGroup) {
	defer fetchGroup.Done()
	// Set timeout details for HTTP GET requests
	transport := &httpclient.Transport{
		// Prime times are useful as one can see when there's an obvious bottleneck
		ConnectTimeout:        7 * time.Second,
		RequestTimeout:        9 * time.Second,
		ResponseHeaderTimeout: 11 * time.Second,
		// After we use the connection once, we won't be using it again as robots.txt is all we want
		DisableKeepAlives: true,
		// In Go 1.2.1, short gzip body responses can result in failures and leaking connections
		// See https://codereview.appspot.com/84850043 for more details
		DisableCompression: true,
	}
	defer transport.Close()
	// Following test in src/pkg/net/http/client_test
	var lastVia []*http.Request
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			lastVia = via
			if len(via) > 10 {
				return errors.New("stopped after 10 redirects")
			}
			return nil
		},
	}
	//
	for fr := range fetchPipeline {
		lastVia = []*http.Request{}
		domain := fr.Domain
		fr.Attempt += 1
		if fr.Attempt > 2 {
			log.Error(fmt.Sprintf("FTCH: Maximum attempts reached for %s", domain))
			continue
		}
		log.Debug(fmt.Sprintf("FTCH: Fetching %s on attempt %d", domain, fr.Attempt))
		// RFC[3.1] states robots.txt must be accessible via HTTP
		urlPath := "http://" + domain + "/robots.txt"
		req, _ := http.NewRequest("GET", urlPath, nil)
		resp, err := client.Do(req)
		// TODO: All these can cause a failure at the end by submitting to the pipeline after it's closed
		if err != nil {
			if urlErr, ok := err.(*url.Error); ok {
				if netErr, ok := (urlErr.Err).(*net.OpError); ok {
					if _, ok := (netErr.Err).(*net.DNSError); ok && !strings.HasPrefix(domain, "www.") {
						log.Warning(fmt.Sprintf("DNS error: %s: trying with an added www", domain))
						fr.Domain = "www." + fr.Domain
						fetchPipeline <- fr
						continue
					}
					if netErr.Timeout() || netErr.Temporary() {
						log.Warning(fmt.Sprintf("Restarting request: %s: timeout / temporary issue... %v", domain, netErr))
						fetchPipeline <- fr
						continue
					}
				}
			}
			// Otherwise, save as domain with no URL -- implies extreme badness
			savePipeline <- RobotResponse{domain, "", false, time.Now(), nil, 0}
			log.Warning(fmt.Sprintf("%s", err))
			continue
		}
		// Work out what the final request URL is
		finalUrl := resp.Request.URL.String()
		// RFC[3.1] states 2xx should be considered success
		if resp.StatusCode < 200 || resp.StatusCode > 206 {
			savePipeline <- RobotResponse{domain, finalUrl, false, time.Now(), nil, len(lastVia)}
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
			continue
		}
		// RFC[3.1] states robots.txt should be text/plain
		// TODO: Handle silly sites like http://www.weibo.com/robots.txt => text/html
		isPlaintext := true
		for _, mtype := range resp.Header["Content-Type"] {
			if !strings.HasPrefix(mtype, "text/plain") {
				isPlaintext = false
				break
			}
		}
		if !isPlaintext {
			savePipeline <- RobotResponse{domain, finalUrl, false, time.Now(), nil, len(lastVia)}
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
			continue
		}
		//
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Warning(fmt.Sprintf("%s", err))
			fetchPipeline <- fr
			continue
		}
		resp.Body.Close()
		savePipeline <- RobotResponse{domain, finalUrl, true, time.Now(), body, len(lastVia)}
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
			body TEXT,
			redirects TEXT
			)`
		_, err = db.Exec(createSql)
		if err != nil {
			log.Fatal(err)
		}
	}
	rows.Close()

	// Ticker -- commit once per second
	delay := 1 * time.Second
	tick := time.Tick(delay)
	saveCount := 0
	failCount := 0
	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	// Generate statement to insert entries
	insertSql, err := tx.Prepare(`insert into
		robots(domain, url, hasRobots, fetchTime, body, redirects)
		values(?, ?, ?, ?, ?, ?)`)
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
				resp.Redirects,
			)
			if err != nil {
				log.Fatal(err)
			}
			if resp.Url == "" {
				failCount += 1
			}
			saveCount += 1
			log.Debug(fmt.Sprintf("SV: Saved %s", resp.Domain))
		case <-tick:
			log.Notice("Saving... %d in %s\n", saveCount, delay)
			log.Notice("Failing... %d in %s\n", failCount, delay)
			saveCount = 0
			failCount = 0
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
				robots(domain, url, hasRobots, fetchTime, body, redirects)
				values(?, ?, ?, ?, ?, ?)`)
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
	//logging.SetLevel(logging.DEBUG, "gogorobot")
	//
	// Set the file descriptor limit higher if we've permission
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Info(fmt.Sprintf("Error geting rlimit: %s", err))
	}
	rLimit.Max = 65536
	rLimit.Cur = 65536
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Info(fmt.Sprintf("Error setting rlimit: %s", err))
	}
	//
	fetchPipeline := make(chan FetchRequest)
	savePipeline := make(chan RobotResponse)
	// NOTE: Important to pass via pointer
	// Otherwise, a new WaitGroup is created
	var fetchGroup sync.WaitGroup
	var saveGroup sync.WaitGroup

	saveGroup.Add(1)
	go saveRobots(savePipeline, &saveGroup)

	for i := 0; i < 50; i++ {
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
		fetchPipeline <- FetchRequest{domain, 0}
	}
	// TODO: Temporary fix to allow time for the pipeline to clear (see TODO in fetcher's error area)
	time.Sleep(60 * time.Second)
	close(fetchPipeline)
	log.Notice("Fetching pipeline closed -- waiting for pending fetches to complete")
	//
	fetchGroup.Wait()
	close(savePipeline)
	saveGroup.Wait()
}
