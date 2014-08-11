package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ClockValue int64

type Value struct {
	Key   string
	Value []byte
	Clock ClockValue
}

type ConsulKVCache struct {
	prefix string

	cache map[string]*Value

	lock sync.RWMutex

	clock ClockValue

	exit bool
}

// Create a new cache for the keys under 'prefix'. The methods
// will add prefix automatically to requests.
func NewConsulKVCache(prefix string) *ConsulKVCache {
	return &ConsulKVCache{
		prefix: prefix + "/",
		cache:  make(map[string]*Value),
		clock:  0,
		exit:   false,
	}
}

func (c *ConsulKVCache) Clock() ClockValue {
	c.lock.RLock()

	val := c.clock

	c.lock.RUnlock()

	return val
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

	idx, _ := strconv.Atoi(resp.Header.Get("X-Consul-Index"))

	c.clock = ClockValue(idx)

	tbl := make(map[string]*Value)

	for _, val := range values {
		tbl[val.Key] = &Value{val.Key, val.Value, ClockValue(val.ModifyIndex)}
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

		c.clock = ClockValue(idx)

		plen := len(c.prefix)

		for _, val := range values {
			c.cache[val.Key] = &Value{val.Key[plen:], val.Value, ClockValue(val.ModifyIndex)}
		}

		c.lock.Unlock()
	}
}

// Retrieve a value relative to the configured
// prefix. Only consults local data.
func (c *ConsulKVCache) Get(key string) (*Value, bool) {
	c.lock.RLock()

	b, ok := c.cache[c.prefix+key]

	c.lock.RUnlock()

	return b, ok
}

func (c *ConsulKVCache) GetPrefix(prefix string) ([]*Value, ClockValue) {
	c.lock.RLock()

	var values []*Value

	var max ClockValue

	prefix = c.prefix + prefix

	for k, v := range c.cache {
		if strings.HasPrefix(k, prefix) {
			if v.Clock > max {
				max = v.Clock
			}

			values = append(values, v)
		}
	}

	c.lock.RUnlock()
	return values, max
}

// Set a value. This sets the value in consul as well.
func (c *ConsulKVCache) Set(key string, val []byte) error {
	c.lock.Lock()

	xkey := c.prefix + key

	c.cache[xkey] = &Value{key, val, c.clock}

	err := setConsulKV(xkey, val)

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
