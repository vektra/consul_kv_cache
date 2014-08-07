package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type ConsulKVCache struct {
	prefix string

	cache map[string][]byte

	lock sync.RWMutex

	exit bool
}

// Create a new cache for the keys under 'prefix'. The methods
// will add prefix automatically to requests.
func NewConsulKVCache(prefix string) *ConsulKVCache {
	return &ConsulKVCache{
		prefix: prefix + "/",
		cache:  make(map[string][]byte),
		exit:   false,
	}
}

// Return how many entries the cache contains.
func (c *ConsulKVCache) Size() int {
	c.lock.RLock()

	sz := len(c.cache)

	c.lock.RUnlock()

	return sz
}

// Frees any resources associated with the cache including
// background goroutines.
func (c *ConsulKVCache) Close() {
	c.exit = true
}

// Read the data out of consul and replace the local cache
// exclusively with the remote values.
func (c *ConsulKVCache) Repopulate() error {
	url := "http://localhost:8500/v1/kv/" + c.prefix + "?recurse"

	resp, err := http.Get(url)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	var values []consulValue

	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&values)
	if err != nil {
		return err
	}

	if len(values) == 0 {
		return nil
	}

	c.lock.Lock()

	tbl := make(map[string][]byte)

	for _, val := range values {
		tbl[val.Key] = val.Value
	}

	c.cache = tbl

	c.lock.Unlock()

	return nil
}

// Update the cached data in the background. This function is meant
// to be started as a goroutine so that it keeps your data in sync
// automatically.
func (c *ConsulKVCache) BackgroundUpdate() {
	url := "http://localhost:8500/v1/kv/" + c.prefix + "?recurse"

	idx := 0

	for {
		resp, err := http.Get(fmt.Sprintf("%s&index=%d&wait=5m", url, idx))

		if c.exit {
			return
		}

		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		idx, _ = strconv.Atoi(resp.Header.Get("X-Consul-Index"))

		defer resp.Body.Close()

		var values []consulValue

		dec := json.NewDecoder(resp.Body)
		err = dec.Decode(&values)
		if err != nil && err != io.EOF {
			time.Sleep(1 * time.Second)
			continue
		}

		if len(values) == 0 {
			continue
		}

		// check for the __sync meta key

		fullSync := false

		for _, val := range values {
			_, base := filepath.Split(val.Key)
			if base == "__sync" {
				fullSync = true
				break
			}
		}

		if fullSync {
			c.Repopulate()
			continue
		}

		c.lock.Lock()

		for _, val := range values {
			c.cache[val.Key] = val.Value
		}

		c.lock.Unlock()
	}
}

// Retrieve a value relative to the configured
// prefix. Only consults local data.
func (c *ConsulKVCache) Get(key string) ([]byte, bool) {
	c.lock.RLock()

	b, ok := c.cache[c.prefix+key]

	c.lock.RUnlock()
	return b, ok
}

// Set a value. This sets the value in consul as well.
func (c *ConsulKVCache) Set(key string, val []byte) error {
	c.lock.Lock()

	key = c.prefix + key

	c.cache[key] = val

	err := setConsulKV(key, val)

	c.lock.Unlock()

	return err
}

// Deletes a value. This deletes the value in consul
// as well.
func (c *ConsulKVCache) Delete(key string) error {
	c.lock.Lock()

	key = c.prefix + key

	delete(c.cache, key)

	delConsulKV(key, false)
	err := setConsulKV(c.prefix+"__sync", []byte("1"))

	c.lock.Unlock()

	return err
}
