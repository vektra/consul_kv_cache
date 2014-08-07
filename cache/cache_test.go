package cache

import (
	"bytes"
	"testing"
	"time"
)

func TestConsulKVCache(t *testing.T) {
	defer delConsulKV("foo", true)

	cache := NewConsulKVCache("foo")

	defer cache.Close()

	if cache.Size() != 0 {
		t.Fatal("why are there values here?")
	}

	m1 := []byte("hello")

	setConsulKV("foo/bar", m1)

	cache.Repopulate()

	got, ok := cache.Get("bar")
	if !ok {
		t.Fatal("cache didn't update")
	}

	if !bytes.Equal(got, m1) {
		t.Fatal("message was corrupt")
	}
}

func TestConsulKVCacheBackgroundUpdate(t *testing.T) {
	defer delConsulKV("foo", true)

	cache := NewConsulKVCache("foo")

	defer cache.Close()

	go cache.BackgroundUpdate()

	if cache.Size() != 0 {
		t.Fatal("why are there values here?")
	}

	m1 := []byte("hello")

	setConsulKV("foo/bar", m1)

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	got, ok := cache.Get("bar")
	if !ok {
		t.Fatal("cache didn't update")
	}

	if !bytes.Equal(got, m1) {
		t.Fatal("message was corrupt")
	}

}

func TestConsulKVCacheBackgroundUpdateDetectsDeletes(t *testing.T) {
	defer delConsulKV("foo", true)

	cache := NewConsulKVCache("foo")

	defer cache.Close()

	go cache.BackgroundUpdate()

	if cache.Size() != 0 {
		t.Fatal("why are there values here?")
	}

	m1 := []byte("hello")

	setConsulKV("foo/bar", m1)
	setConsulKV("foo/baz", m1)

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	if cache.Size() != 2 {
		t.Fatal("didn't pick up both keys")
	}

	delConsulKV("foo/baz", false)
	setConsulKV("foo/__sync", []byte("now"))

	// propogation delay
	time.Sleep(100 * time.Millisecond)

	_, ok := cache.Get("baz")
	if ok {
		t.Fatal("baz didn't go away")
	}
}

func TestConsulKVCacheBackgroundSet(t *testing.T) {
	defer delConsulKV("foo", true)

	cache := NewConsulKVCache("foo")

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

	if !bytes.Equal(got, m1) {
		t.Fatal("message was corrupt")
	}
}

func TestConsulKVCacheDelete(t *testing.T) {
	defer delConsulKV("foo", true)

	cache := NewConsulKVCache("foo")

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