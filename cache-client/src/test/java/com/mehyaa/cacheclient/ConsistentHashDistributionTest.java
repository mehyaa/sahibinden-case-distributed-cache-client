package com.mehyaa.cacheclient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsistentHashDistributionTest {

    @Test
    public void testKeyDistributionBalance() {
        // Prepare nodes and ring
        List<String> nodes = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            nodes.add("node" + i + ":80");
        }

        ConsistentHash ring = new ConsistentHash(nodes);

        // Generate many keys
        final int keys = 50_000; // reasonable for CI speed

        List<String> keyList = new ArrayList<>(keys);

        for (int i = 0; i < keys; i++) {
            keyList.add("key-" + i);
        }

        // Map keys to nodes and count
        Map<String, Integer> counts = new HashMap<>();

        for (String node : nodes) {
            counts.put(node, 0);
        }

        for (String k : keyList) {
            String node = ring.get(k);

            // node should not be null if there are nodes
            if (node != null) {
                counts.put(node, counts.get(node) + 1);
            }
        }

        // Compute statistics
        int min = Collections.min(counts.values());
        int max = Collections.max(counts.values());
        double avg = (double) keys / nodes.size();

        double maxDeviation = (max - avg) / avg; // fraction above average
        double minDeviation = (avg - min) / avg; // fraction below average

        System.out.println("Counts: " + counts.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ")));

        System.out.println(String.format("min=%d max=%d avg=%.2f maxDev=%.4f minDev=%.4f", min, max, avg, maxDeviation,
                minDeviation));

        // Assert that distribution is reasonably balanced: no more than 12% deviation
        // from average
        assertTrue(maxDeviation < 0.12, "Max deviation too high: " + maxDeviation);
        assertTrue(minDeviation < 0.12, "Min deviation too high: " + minDeviation);
    }
}