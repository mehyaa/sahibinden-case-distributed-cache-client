package com.mehyaa.cacheclient;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsistentHashConcurrencyTest {

    @Test
    public void stressConcurrentGetsWhileMutating() throws Exception {
        List<String> initial = Arrays.asList("node1:80", "node2:80", "node3:80", "node4:80");
        final ConsistentHash ring = new ConsistentHash(initial);

        final int readers = 16;
        final int writers = 2;
        final int durationSeconds = 3;

        ExecutorService exec = Executors.newFixedThreadPool(readers + writers);

        final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        final AtomicBoolean running = new AtomicBoolean(true);
        final Random rnd = new Random(12345);

        // start reader tasks
        for (int i = 0; i < readers; i++) {
            exec.submit(() -> {
                try {
                    while (running.get()) {
                        String key = "key-" + rnd.nextInt(1000);

                        try {
                            ring.get(key); // should not throw
                        } catch (Throwable t) {
                            failures.add(t);
                        }
                    }
                } catch (Throwable t) {
                    failures.add(t);
                }
            });
        }

        // start writer tasks (add/remove nodes)
        for (int i = 0; i < writers; i++) {
            final int writerId = i;

            exec.submit(() -> {
                try {
                    int idx = 0;

                    while (running.get()) {
                        String node = "writer" + writerId + "-" + idx + ":80";

                        // alternate add/remove
                        if (idx % 2 == 0) {
                            try {
                                ring.add(node);
                            } catch (Throwable t) {
                                failures.add(t);
                            }
                        } else {
                            try {
                                ring.remove(node);
                            } catch (Throwable t) {
                                failures.add(t);
                            }
                        }

                        idx++;

                        // small pause
                        Thread.sleep(5);
                    }
                } catch (Throwable t) {
                    failures.add(t);
                }
            });
        }

        // run for durationSeconds
        Thread.sleep(TimeUnit.SECONDS.toMillis(durationSeconds));
        running.set(false);

        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);

        // Assert no exceptions were recorded
        assertTrue(failures.isEmpty(), "No exceptions should occur during concurrent access; found: " + failures.size());
    }
}