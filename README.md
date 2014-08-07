Consul KV Cache
===============

It's common to use data stored within consul's kv store for operations
that need to be faster than doing an HTTP roundtrip each time.

For those times, there is the Consul KV Cache.

It's BackgroundUpdate function uses the watch capability to automatically
keep a local map of data in sync.

Cavaet
======

Right now, consul does not fire recurse watches for deleted keys where
other entries still exist under the prefix.

To work around this limitation, the KV cache uses a special key called
**__sync**. If a watch fires for a key with the base name of **__sync**
then the cache will be completely repopulated from consul.

Additionally, using the Delete function on the cache will set **__sync**
so that other caches elsewhere will know to perform a full re-read of
the data.
