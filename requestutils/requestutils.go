package requestutils

import (
	"github.com/garyburd/redigo/redis"
	"encoding/json"
	"net/http"
	"io"
	"strings"
)

const (
	BIDDING = 0
	REDUCING = 1
	COMPLETE = 2
)

type UrlInfo struct {
	Url string
	Code int
}

type Request struct {
	Status int
	InputUrls []UrlInfo
	Result map[int][]string
}

func (r Request) Serialize() []byte {
	serialized, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return serialized
}

func (r *Request) Deserialize(b []byte) {
	err := json.Unmarshal(b, r)
	if err != nil {
		panic(err)
	}
}

func Save(conn redis.Conn, id int, req *Request) {
	serialized := req.Serialize()
	conn.Do("SET", id, serialized)
}

func Load(conn redis.Conn, id int) (*Request, error) {
	serialized, err := redis.Bytes(conn.Do("GET", id))
	if err != nil {
		return nil, err
	}
	req := &Request{}
	req.Deserialize(serialized)
	return req, nil
}

func CreateObjectFromFile(r *http.Request) (*Request, error) {
	contentReader, err := r.MultipartReader()
	if err != nil {
		return nil, err
	}
	filecontents := ""
	content := make([]byte, 100)
	for {
		part, part_err := contentReader.NextPart()
		if part_err == io.EOF {
			break
		}
		for {				
			bytes_read, part_err := part.Read(content)
			if part_err != nil {
				break
			}
			filecontents += string(content[:bytes_read])
		}
	}
	URLs := strings.Split(filecontents, "\n")
	request := Request{Status: BIDDING}
	for _, v := range URLs {
		request.InputUrls = append(request.InputUrls, UrlInfo{Url: v})
	}
	return &request, nil
}