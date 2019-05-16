package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"

	"github.com/olivere/elastic/v7"
)

var pool, writer_p sync.WaitGroup
var esUrl string

type Page struct {
	id      int
	index   string
	url     string
	header  string
	body    string
	ip_addr string
	alexa   int
	admin   string
	robots  string
}

type esPage struct {
	id      int
	index   string
	Url     string `json:"url"`
	Header  string `json:"header"`
	Body    string `json:"body"`
	Ip_addr string `json:"ip_addr"`
	Alexa   int    `json:"alexa"`
	Admin   string `json:"admin"`
	Robots  string `json:"robots"`
}

func worker(id int, jobs <-chan Page, out chan<- Page) {
	defer pool.Done()
	for job := range jobs {
		var work Page
		work = job
		fmt.Printf("Starting:%s\n", work.url)
		work.body, work.header = get(work.url)
		if work.body == "" {
			continue
		}
		fmt.Printf("Starting:%s/admin/\n", work.url)
		work.admin, _ = get(work.url + "/admin/")
		fmt.Printf("Starting:%s/robots.txt\n", work.url)
		work.robots, _ = get(work.url + "/robots.txt")
		fmt.Printf("Starting get IP: %s\n", work.url)
		ip, _ := net.LookupIP(work.url)
		work.ip_addr = ip[0].String()
		//		fmt.Println("W:",id,"data",job.url)
		out <- work
	}
	fmt.Println("Theread", id, "finished")
}

func writer(data <-chan Page, client *elastic.Client) {
	defer writer_p.Done()
	for rec := range data {
		fmt.Printf("index:%s url:%s alexa:%d\n", rec.index, rec.url, rec.alexa)
		index(rec, client)
	}
	fmt.Println("Writer ended")
}

func get(url string) (string, string) {
	var h bytes.Buffer
	client := &http.Client{}
	req, err := http.NewRequest("GET", "http://"+url, nil)
	if err != nil {
		log.Fatalln("Error(get):", err)
		return "", ""
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows ME; rv:77.0) Gecko/20100101 Firefox/77.0")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error(get): ", err)
		return "", ""
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	for k, v := range resp.Header {
		h.WriteString(string(k) + ": " + string(v[0]) + "\n")
	}
	b := string(body)
	return b, h.String()
}

func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func index(page Page, client *elastic.Client) {
	index := page.index
	toES := esPage{
		Url:     page.url,
		Header:  page.header,
		Body:    page.body,
		Ip_addr: page.ip_addr,
		Alexa:   page.alexa,
		Admin:   page.admin,
		Robots:  page.robots,
	}
	_, err := client.Index().
		Index(index).
		Id(strconv.Itoa(page.id)).
		BodyJson(&toES).
		Refresh("true").
		Do(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
}

func createIndex(client *elastic.Client, index string) {
	mapping := `{
		"settings":{
			"number_of_shards":5,
			"number_of_replicas":0
		},
		"mappings":{
			"properties":{
				"url":{
					"type":"text"
				},
				"header":{
					"type":"text"
				},
				"body":{
					"type":"text"
				},
				"ip_addr":{
					"type":"ip"
				},
				"alexa":{
					"type":"integer"
				},
				"admin":{
					"type":"text"
				},
				"robots":{
					"type":"text"
				}
			}
		}
	}`

	ctx := context.Background()
	_, err := client.CreateIndex(index).BodyString(mapping).Do(ctx)
	if err != nil {
		panic(err)
	}
}

func deleteIndex(client *elastic.Client, index string) {
	exists, err := client.IndexExists(index).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if exists {
		ctx := context.Background()
		_, err := client.DeleteIndex(index).Do(ctx)
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	urlsFile := flag.String("src", "./src.lst", "domain list")
	esUrl = *flag.String("url", "http://127.0.0.1:9200", "ElasticSearch url")
	indexName := flag.String("index", "test", "Index name")
	threads := flag.Int("threads", 100, "workers count")
	CreateIndexOnly := flag.Bool("cio", false, "Only create index")
	flag.Parse()

	client, err := elastic.NewClient(elastic.SetURL(esUrl), elastic.SetTraceLog(log.New(os.Stderr, "", 0)))
	if err != nil {
		log.Fatal("Could not connect to elasticsearch", err)
		os.Exit(1)
	}
	defer client.Stop()
	deleteIndex(client, *indexName)
	createIndex(client, *indexName)
	if *CreateIndexOnly == true {
		os.Exit(0)
	}

	fmt.Println(*urlsFile)
	src, _ := os.Open(*urlsFile)
	defer src.Close()
	count, _ := lineCounter(src)
	src.Seek(0, io.SeekStart)
	queueIn := make(chan Page, count)
	queueOut := make(chan Page, count)
	scanner := bufio.NewScanner(src)
	reAlexa := regexp.MustCompile("^[^,]+")
	reUrl := regexp.MustCompile("[^,]+$")
	pool.Add(*threads)
	writer_p.Add(1)
	id := 0
	for scanner.Scan() {
		var job Page
		job.id = id
		job.url = reUrl.FindString(scanner.Text())
		job.index = *indexName
		fmt.Sscan(reAlexa.FindString(scanner.Text()), &job.alexa)
		queueIn <- job
		id++
	}

	for i := 0; i < *threads; i++ {
		go worker(i, queueIn, queueOut)
	}
	close(queueIn)
	go writer(queueOut, client)
	pool.Wait()
	close(queueOut)
	writer_p.Wait()
}
