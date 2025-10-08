package com.mehyaa.cacheclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsistentHashStabilityTest {

    @Test
    public void testSingleNodeAddRemoveStability() {
        // initial nodes
        List<String> nodes = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            nodes.add("node" + i + ":80");
        }

        ConsistentHash ring = new ConsistentHash(nodes);

        final int keys = 50_000;
        List<String> keyList = new ArrayList<>(keys);

        for (int i = 0; i < keys; i++) {
            keyList.add("key-" + i);
        }

        // mapping before
        Map<String, String> before = new HashMap<>();

        for (String k : keyList) {
            before.put(k, ring.get(k));
        }

        // add a single node
        String added = "node-new:80";
        ring.add(added);

        Map<String, String> afterAdd = new HashMap<>();

        for (String k : keyList) {
            afterAdd.put(k, ring.get(k));
        }

        long movedAfterAdd = 0;
        for (String k : keyList) {
            String b = before.get(k);
            String a = afterAdd.get(k);

            if (!Objects.equals(b, a)) {
                movedAfterAdd++;
            }
        }

        double fractionMovedAdd = (double) movedAfterAdd / keys;

        // expected fraction approx 1/(N+1)
        double expected = 1.0 / (nodes.size() + 1);

        System.out.println(String.format("movedAfterAdd=%d fraction=%.4f expected≈%.4f", movedAfterAdd,
                fractionMovedAdd, expected));

        // allow some tolerance but ensure most keys are stable
        assertTrue(fractionMovedAdd <= Math.max(0.20, expected * 2.0),
                "Too many keys moved after add: " + fractionMovedAdd);

        // now remove the same node
        ring.remove(added);

        Map<String, String> afterRemove = new HashMap<>();

        for (String k : keyList) {
            afterRemove.put(k, ring.get(k));
        }

        long movedAfterRemove = 0;
        for (String k : keyList) {
            String b = before.get(k);
            String a = afterRemove.get(k);

            if (!Objects.equals(b, a)) {
                movedAfterRemove++;
            }
        }

        double fractionMovedRemove = (double) movedAfterRemove / keys;

        System.out.println(String.format("movedAfterRemove=%d fraction=%.4f", movedAfterRemove, fractionMovedRemove));

        // removal should also be reasonably small
        assertTrue(fractionMovedRemove <= Math.max(0.20, expected * 2.0),
                "Too many keys moved after remove: " + fractionMovedRemove);
    }
}