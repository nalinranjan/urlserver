package main

import (
	"fmt"
	"strconv"
	"net/http"
	"net/http/httputil"
	"encoding/json"
	"urlserver/requestutils"
	"github.com/garyburd/redigo/redis"
)

func newRedisPool() *redis.Pool {
	return &redis.Pool {
		Dial: func() (redis.Conn, error) {
			c, er := redis.Dial("tcp", ":6379")
			if er != nil {
				panic(er)
			}
			return c, er
		},
	}
}

var (
	nextReqId = 1
	pool = newRedisPool()
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
			return
		}
		requestutils.Save(conn, id, request)
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
			result := json.MarshalIndent(request.Result, "", "    ")
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
	conn := pool.Get()
	defer conn.Close()

	http.HandleFunc("/requests/", requestHandler)
	http.ListenAndServe(":8000", nil)
}