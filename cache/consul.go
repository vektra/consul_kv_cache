package cache

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
)

var bTrue = []byte("true\n")

type consulValue struct {
	CreateIndex int
	ModifyIndex int
	Key         string
	Flags       int
	Value       []byte
}

func setConsulKV(key string, value []byte) error {
	url := "http://localhost:8500/v1/kv/" + key

	body := bytes.NewReader(value)

	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return err
	}

	req.ContentLength = int64(len(value))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)

	if !bytes.Equal(data, bTrue) {
		errors.New("Consul returned an error setting the value")
	}

	return nil
}

func delConsulKV(key string, rec bool) error {
	url := "http://localhost:8500/v1/kv/" + key

	if rec {
		url += "?recurse"
	}

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	resp.Body.Close()

	return nil
}
