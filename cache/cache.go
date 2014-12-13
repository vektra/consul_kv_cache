package cache

import (
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/armon/consul-api"
)

type ClockValue int64

type Value struct {
	Key   string
	Value []byte
	Clock ClockValue
}

type ConsulKVCache struct {
	consul *consulapi.KV
	prefix string

	lock  sync.RWMutex
	cache map[string]*Value
	clock ClockValue

	exit bool
}

// Create a new cache for the keys under 'prefix'. The methods
// will add prefix automatically to requests.
func NewConsulKVCache(prefix string) *ConsulKVCache {
	return NewCustomConsulKVCache(prefix, nil)
}

// Like NewConsulKVCache, only it supports passing a customized api client.
func NewCustomConsulKVCache(prefix string, client *consulapi.Client) *ConsulKVCache {
	if client == nil {
		client, _ = consulapi.NewClient(consulapi.DefaultConfig())
	}

	return &ConsulKVCache{
		consul: client.KV(),
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
	values, meta, err := c.consul.List(c.prefix, nil)
	if err != nil {
		return err
	}

	if len(values) == 0 {
		return nil
	}

	c.lock.Lock()

	c.clock = ClockValue(meta.LastIndex)

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
	plen := len(c.prefix)
	opts := consulapi.QueryOptions{
		WaitTime: 5 * time.Minute,
	}

	for {
		values, meta, err := c.consul.List(c.prefix, &opts)

		if c.exit {
			return
		}

		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		opts.WaitIndex = meta.LastIndex

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

		c.clock = ClockValue(meta.LastIndex)

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
	var (
		values []*Value
		max    ClockValue
	)

	prefix = c.prefix + prefix

	c.lock.RLock()

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
	xkey := c.prefix + key

	c.lock.Lock()

	c.cache[xkey] = &Value{key, val, c.clock}

	err := c.setConsulKV(xkey, val)

	c.lock.Unlock()
	return err
}

// Deletes a value. This deletes the value in consul
// as well.
func (c *ConsulKVCache) Delete(key string) error {
	key = c.prefix + key

	c.lock.Lock()

	delete(c.cache, key)

	c.delConsulKV(key, false)
	err := c.setConsulKV(c.prefix+"__sync", []byte("1"))

	c.lock.Unlock()
	return err
}

func (c *ConsulKVCache) setConsulKV(key string, value []byte) error {
	pair := consulapi.KVPair{Key: key, Value: value}

	if _, err := c.consul.Put(&pair, nil); err != nil {
		return err
	}

	return nil
}

func (c *ConsulKVCache) delConsulKV(key string, rec bool) (err error) {
	if rec {
		_, err = c.consul.DeleteTree(key, nil)
	} else {
		_, err = c.consul.Delete(key, nil)
	}
	return
}
