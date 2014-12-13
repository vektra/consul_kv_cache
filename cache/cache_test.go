package cache

import (
	"bytes"
	"testing"
	"time"
)

func TestConsulKVCache(t *testing.T) {
	cache := NewConsulKVCache("foo")

	defer cache.delConsulKV("foo", true)
	defer cache.Close()

	if cache.Size() != 0 {
		t.Fatal("why are there values here?")
	}

	m1 := []byte("hello")

	cache.setConsulKV("foo/bar", m1)

	cache.Repopulate()

	got, ok := cache.Get("bar")
	if !ok {
		t.Fatal("cache didn't update")
	}

	if !bytes.Equal(got.Value, m1) {
		t.Fatal("message was corrupt")
	}
}

func TestConsulKVCacheBackgroundUpdate(t *testing.T) {
	cache := NewConsulKVCache("foo")

	defer cache.delConsulKV("foo", true)
	defer cache.Close()

	go cache.BackgroundUpdate()

	if cache.Size() != 0 {
		t.Fatal("why are there values here?")
	}

	m1 := []byte("hello")

	cache.setConsulKV("foo/bar", m1)

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	got, ok := cache.Get("bar")
	if !ok {
		t.Fatal("cache didn't update")
	}

	if !bytes.Equal(got.Value, m1) {
		t.Fatal("message was corrupt")
	}
}

func TestConsulKVCacheBackgroundUpdateDetectsDeletes(t *testing.T) {
	cache := NewConsulKVCache("foo")

	defer cache.delConsulKV("foo", true)
	defer cache.Close()

	go cache.BackgroundUpdate()

	if cache.Size() != 0 {
		t.Fatal("why are there values here?")
	}

	m1 := []byte("hello")

	cache.setConsulKV("foo/bar", m1)
	cache.setConsulKV("foo/baz", m1)

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	if cache.Size() != 2 {
		t.Fatal("didn't pick up both keys")
	}

	cache.delConsulKV("foo/baz", false)
	cache.setConsulKV("foo/__sync", []byte("now"))

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	_, ok := cache.Get("baz")
	if ok {
		t.Fatal("baz didn't go away")
	}
}

func TestConsulKVCacheDeletesDontChangeUnrelatedClocks(t *testing.T) {
	cache := NewConsulKVCache("foo")

	defer cache.delConsulKV("foo", true)
	defer cache.Close()

	go cache.BackgroundUpdate()

	if cache.Size() != 0 {
		t.Fatal("why are there values here?")
	}

	m1 := []byte("hello")

	cache.setConsulKV("foo/bar", m1)
	cache.setConsulKV("foo/baz", m1)

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	if cache.Size() != 2 {
		t.Fatal("didn't pick up both keys")
	}

	bar, ok := cache.Get("bar")
	if !ok {
		t.Fatal("couldn't find bar")
	}

	cache.delConsulKV("foo/baz", false)
	cache.setConsulKV("foo/__sync", []byte("now"))

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	_, ok = cache.Get("baz")
	if ok {
		t.Fatal("baz didn't go away")
	}

	bar2, ok := cache.Get("bar")
	if !ok {
		t.Fatal("couldn't find bar")
	}

	if bar.Clock != bar2.Clock {
		t.Fatal("delete caused clock change")
	}
}

func TestConsulKVCacheBackgroundSet(t *testing.T) {
	cache := NewConsulKVCache("foo")

	defer cache.delConsulKV("foo", true)
	defer cache.Close()

	go cache.BackgroundUpdate()

	c2 := NewConsulKVCache("foo")

	m1 := []byte("hello")

	c2.Set("bar", m1)

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	got, ok := cache.Get("bar")
	if !ok {
		t.Fatal("cache didn't update")
	}

	if !bytes.Equal(got.Value, m1) {
		t.Fatal("message was corrupt")
	}
}

func TestConsulKVCacheDelete(t *testing.T) {
	cache := NewConsulKVCache("foo")

	defer cache.delConsulKV("foo", true)
	defer cache.Close()

	go cache.BackgroundUpdate()

	c2 := NewConsulKVCache("foo")

	m1 := []byte("hello")

	c2.Set("bar", m1)
	c2.Set("baz", m1)

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	c2.Delete("baz")

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	_, ok := cache.Get("baz")
	if ok {
		t.Fatal("baz didn't go away")
	}
}

func TestConsulKVCacheGet(t *testing.T) {
	cache := NewConsulKVCache("foo")

	defer cache.delConsulKV("foo", true)
	defer cache.Close()

	go cache.BackgroundUpdate()

	c2 := NewConsulKVCache("foo")

	m1 := []byte("hello")

	c2.Set("c/bar", m1)
	c2.Set("c/baz", m1)

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	values, _ := cache.GetPrefix("c")

	if len(values) != 2 {
		t.Fatal("Didn't find the right values")
	}

	if values[0].Key == "c/bar" {
		if values[1].Key != "c/baz" {
			t.Fatal("didn't get the right values")
		}
	} else if values[0].Key == "c/baz" {
		if values[1].Key != "c/bar" {
			t.Fatal("didn't get the right values")
		}
	} else {
		t.Fatal("wrong keys all together")
	}

	if !bytes.Equal(values[0].Value, m1) {
		t.Fatal("corrupted values")
	}

	if !bytes.Equal(values[1].Value, m1) {
		t.Fatal("corrupted values")
	}
}

func TestConsulKVCacheClocked(t *testing.T) {
	cache := NewConsulKVCache("foo")

	defer cache.delConsulKV("foo", true)
	defer cache.Close()

	go cache.BackgroundUpdate()

	c2 := NewConsulKVCache("foo")

	start := cache.Clock()

	m1 := []byte("hello")

	c2.Set("c/bar", m1)
	c2.Set("c/baz", m1)

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	values, max := cache.GetPrefix("c")

	if len(values) != 2 {
		t.Fatal("Didn't find the right values")
	}

	if values[0].Clock <= start || values[0].Clock > max {
		t.Fatal("clock did not increment")
	}

	if values[1].Clock <= start || values[0].Clock > max {
		t.Fatal("clock did not increment")
	}
}
