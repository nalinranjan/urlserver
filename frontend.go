package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"net/http"
	"net/http/httputil"
	"strconv"
	"urlserver/bidder"
	"urlserver/reducer"
	"urlserver/requestutils"
)

func newRedisPool() *redis.Pool {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, er := redis.Dial("tcp", ":6379")
			if er != nil {
				panic(er)
			}
			return c, er
		},
	}
}

const BUFFER_SIZE = 10

var (
	nextReqId = 1
	pool      = newRedisPool()
	bidding   = make(chan int, BUFFER_SIZE)
	reducing  = make(chan int, BUFFER_SIZE)
)

func requestHandler(w http.ResponseWriter, r *http.Request) {
	data, _ := httputil.DumpRequest(r, true)
	fmt.Println(string(data))

	conn := pool.Get()
	defer conn.Close()

	reqId := r.URL.Path[len("/requests/"):]

	if len(reqId) == 0 {
		id := nextReqId
		nextReqId++
		w.Header().Set("req_id", fmt.Sprintf("%v", id))
		request, err := requestutils.CreateObjectFromFile(r)
		if err != nil {
			fmt.Fprintf(w, "Bad data")
			fmt.Printf("%T %v", err, err)
			return
		}
		requestutils.Save(conn, id, request)
		bidding <- id
	} else {
		id, err := strconv.Atoi(reqId)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		request, err := requestutils.Load(conn, id)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if request.Status == requestutils.COMPLETE {
			result, err := json.MarshalIndent(request.Result, "", "    ")
			if err != nil {
				panic(err)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(result)
		} else {
			status := ""
			switch request.Status {
			case requestutils.BIDDING:
				status = "Bidding"
			case requestutils.REDUCING:
				status = "Reducing"
			}
			w.Header().Set("Retry-After", fmt.Sprintf("%v", 10))
			fmt.Fprintln(w, "Current Status:", status)
		}
	}
}

func main() {
	go bidder.Run(bidding, reducing)
	go reducer.Run(reducing)

	conn := pool.Get()
	defer conn.Close()

	http.HandleFunc("/requests/", requestHandler)
	http.ListenAndServe(":8000", nil)
}
