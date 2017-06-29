package bidder

import (
	"time"
	"net/http"
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

var pool = newRedisPool()

func GetHttpResponseCode(url string, c chan *requestutils.UrlInfo) {
	client := http.Client{Timeout: time.Duration(5*time.Second)}
	resp, err := client.Get(url)
	if err != nil {
		// panic(err)
		c <- &requestutils.UrlInfo{url, 0}
	} else {
		defer resp.Body.Close()
		c <- &requestutils.UrlInfo{url, resp.StatusCode}
	}
}

func Run(bidding, reducing chan int) {
	conn := pool.Get()
	defer conn.Close()

	for {
		select {
		case reqId := <- bidding:
			request, err := requestutils.Load(conn, reqId)
			if err != nil {
				panic (err)
			}
			
			num_urls := len(request.InputUrls)
			completed := make(chan *requestutils.UrlInfo, len(request.InputUrls))
			for _, v := range request.InputUrls {
				go GetHttpResponseCode(v.Url, completed)
			}

			request.InputUrls = []requestutils.UrlInfo{}

			waitloop:
				for {
					select {
					case resp := <-completed:
						request.InputUrls = append(request.InputUrls, *resp)
						if len(request.InputUrls) == num_urls {
							request.Status = requestutils.REDUCING
							requestutils.Save(conn, reqId, request)
							reducing <- reqId
							break waitloop
						}
					case <-time.After(50*time.Millisecond):
					}
				}
		case <-time.After(100*time.Millisecond):
		}
	}
}