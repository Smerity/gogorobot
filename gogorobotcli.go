// Based upon code by jgc (github.com/jgrahamc/dotgo) -- MIT
// https://github.com/cloudflare/jgc-talks/blob/master/dotGo/2014/EasyConcurrencyEasyComposition.pdf

package main

import (
	"bufio"
	_ "crypto/sha512" // See http://bridge.grumpy-troll.org/2014/05/golang-tls-comodo/
	"errors"
	"fmt"
	//httpclient "github.com/mreiferson/go-httpclient"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"encoding/json"
	"log"
)

type task interface {
	process()
	print()
}

type factory interface {
	make(line string) task
}

//

var timeout = time.Duration(5 * time.Second)

func dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, timeout)
}

//
type RobotResponse struct {
	Domain    string
	Url       string
	HasRobots bool
	FetchTime time.Time
	Body      string
	Redirects int
}

func (r *RobotResponse) process() {
	attempt := 0
	transport := &http.Transport{
		Dial: dialTimeout,
		// After we use the connection once, we won't be using it again as robots.txt is all we want
		DisableKeepAlives: true,
		// In Go 1.2.1, short gzip body responses can result in failures and leaking connections
		// See https://codereview.appspot.com/84850043 for more details
		DisableCompression: true,
	}
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
FETCH_START:
	// RFC[3.1] states robots.txt must be accessible via HTTP
	r.Url = "http://" + r.Domain + "/robots.txt"
	resp, err := client.Get(r.Url)
	r.FetchTime = time.Now()
	r.Redirects = len(lastVia)
	if err != nil {
		// Add www. if DNS error, else bail
		if urlErr, ok := err.(*url.Error); ok {
			if netErr, ok := (urlErr.Err).(*net.OpError); ok {
				if _, ok := (netErr.Err).(*net.DNSError); ok && !strings.HasPrefix(r.Domain, "www.") {
					r.Domain = "www." + r.Domain
					goto FETCH_START
				}
			}
		}
		if attempt < 4 {
			attempt += 1
			time.Sleep(time.Duration(1000*attempt) * time.Millisecond)
			goto FETCH_START
		}
		// If it fails repeatedly, it must die
		return
	}
	r.Url = resp.Request.URL.String()
	// RFC[3.1] states 2xx should be considered success
	if resp.StatusCode < 200 || resp.StatusCode > 206 {
		// Clean up
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		return
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
		// Clean up
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		return
	}
	//
	r.HasRobots = true
	success += 1
	body, err := ioutil.ReadAll(resp.Body)
	if err == nil {
		r.Body = string(body)
	}
	resp.Body.Close()
}

func (r *RobotResponse) print() {
	payload, _ := json.Marshal(r)
	fmt.Printf("%s\t%v\t%s\n", r.Domain, r.HasRobots, payload)
}

type FetcherFactory struct{}

func (f *FetcherFactory) make(line string) task {
	return &RobotResponse{line, "", false, time.Now(), "", 0}
}

//

var success int = 0

func run(f factory) {
	var wg sync.WaitGroup

	in := make(chan task)
	taskCount := 0
	WORKERS := 500

	wg.Add(1)
	go func() {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			in <- f.make(s.Text())
			taskCount += 1
			if taskCount%100 == 0 {
				fmt.Fprintf(os.Stderr, "\r%d workers: %d successes with %d attempts (%.2f%%)", WORKERS, success, taskCount, float64(success)/float64(taskCount)*100)
			}
		}
		if s.Err() != nil {
			log.Fatalf("Error reading STDIN: %s", s.Err())
		}
		close(in)
		wg.Done()
	}()

	out := make(chan task)

	for i := 0; i < WORKERS; i++ {
		wg.Add(1)
		go func() {
			for t := range in {
				t.process()
				out <- t
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	for t := range out {
		t.print()
	}
}

func main() {
	run(&FetcherFactory{})
}
