package reducer

import (
	"time"
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

func Run(reducing chan int) {
	conn := pool.Get()
	defer conn.Close()

	for {
		select {
		case reqId := <-reducing:
			request, err := requestutils.Load(conn, reqId)
			if err != nil {
				panic (err)
			}			
			request.Result = make(map[int][]string)
			for _, v := range request.InputUrls {
				request.Result[v.Code] = append(request.Result[v.Code], v.Url)
			}
			request.Status = requestutils.COMPLETE
			requestutils.Save(conn, reqId, request)
		case <-time.After(100*time.Millisecond):
		}
	}
}