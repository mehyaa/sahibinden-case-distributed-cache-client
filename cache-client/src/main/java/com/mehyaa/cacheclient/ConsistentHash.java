package com.mehyaa.cacheclient;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashCode;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Simple consistent hashing implementation with virtual nodes.
 */
public class ConsistentHash {
    private static final int VIRTUAL_NODES = 150; // Number of virtual nodes per real node

    // Volatile reference to an unmodifiable SortedMap. Readers access this without locking.
    private volatile SortedMap<Long, String> circle = Collections.unmodifiableSortedMap(new TreeMap<>());

    public ConsistentHash(List<String> nodes) {
        // build initial map once and publish
        TreeMap<Long, String> initial = new TreeMap<>();

        if (nodes != null) {
            for (String node : nodes) {
                for (int i = 0; i < VIRTUAL_NODES; i++) {
                    long hash = hash(node + "#" + i);
                    initial.put(hash, node);
                }
            }
        }

        circle = Collections.unmodifiableSortedMap(initial);
    }

    /**
     * Adds a node by producing a new map with the additional virtual nodes
     * and atomically publishing it.
     */
    public synchronized void add(String node) {
        TreeMap<Long, String> newMap = new TreeMap<>(circle);

        for (int i = 0; i < VIRTUAL_NODES; i++) {
            long hash = hash(node + "#" + i);
            newMap.put(hash, node);
        }

        circle = Collections.unmodifiableSortedMap(newMap);
    }

    /**
     * Removes a node by producing a new map without that node's virtual nodes
     * and atomically publishing it.
     */
    public synchronized void remove(String node) {
        TreeMap<Long, String> newMap = new TreeMap<>(circle);

        for (int i = 0; i < VIRTUAL_NODES; i++) {
            long hash = hash(node + "#" + i);
            newMap.remove(hash);
        }

        circle = Collections.unmodifiableSortedMap(newMap);
    }

    /**
     * Lock-free read of the current ring.
     */
    public String get(String key) {
        SortedMap<Long, String> current = circle;

        if (current.isEmpty()) {
            return null;
        }

        long hash = hash(key);
        SortedMap<Long, String> tailMap = current.tailMap(hash);
        Long nodeHash = tailMap.isEmpty() ? current.firstKey() : tailMap.firstKey();

        return current.get(nodeHash);
    }

    /**
     *  Hashes the given data using MurmurHash3.
     */
    private long hash(String data) {
        HashCode dataHash = Hashing.murmur3_128().hashBytes(data.getBytes(StandardCharsets.UTF_8));

        // Use lower 64 bits of the 128-bit hash
        return dataHash.asLong();
    }
}