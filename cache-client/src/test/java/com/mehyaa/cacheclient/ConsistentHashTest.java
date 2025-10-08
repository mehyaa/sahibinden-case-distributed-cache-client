package com.mehyaa.cacheclient;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ConsistentHashTest {

    @Test
    public void testDeterministicMappingAcrossInstances() {
        List<String> nodes = Arrays.asList("node1:80", "node2:80", "node3:80");
        List<String> keys = Arrays.asList("alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta");

        ConsistentHash h1 = new ConsistentHash(nodes);
        ConsistentHash h2 = new ConsistentHash(nodes);

        Map<String, String> map1 = new HashMap<>();
        Map<String, String> map2 = new HashMap<>();

        for (String k : keys) {
            map1.put(k, h1.get(k));
            map2.put(k, h2.get(k));
        }

        assertEquals(map1, map2, "Two independently built rings must map keys deterministically the same");
    }

    @Test
    public void testEmptyRingReturnsNull() {
        ConsistentHash h = new ConsistentHash(Arrays.asList());
        assertNull(h.get("anything"));
    }
}