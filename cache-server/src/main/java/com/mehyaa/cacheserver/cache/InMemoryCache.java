package com.mehyaa.cacheserver.cache;

import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryCache {
    private static final InMemoryCache INSTANCE = new InMemoryCache();

    private final ConcurrentHashMap<String, String> cache;

    private InMemoryCache() {
        cache = new ConcurrentHashMap<>();
    }

    public static InMemoryCache getInstance() {
        return INSTANCE;
    }

    public String get(String key) {
        return cache.get(key);
    }

    public void put(String key, String value) {
        cache.put(key, value);
    }

    public void delete(String key) {
        cache.remove(key);
    }
}