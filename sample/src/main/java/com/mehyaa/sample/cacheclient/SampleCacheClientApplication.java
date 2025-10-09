package com.mehyaa.sample.cacheclient;

import com.mehyaa.cacheclient.CacheClient;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleCacheClientApplication {

    private static final Logger logger = LoggerFactory.getLogger(SampleCacheClientApplication.class);

    private static final int NUM_THREADS = 10;
    private static final int TEST_DURATION_SECONDS = 3 * 60; // 3 minutes
    private static final int KEY_SPACE = 100_000;

    public static void main(String[] args) throws Exception {
        try (CacheClient client = CacheClient.getInstance()) {
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

            logger.info("Starting load test with {} threads for {} seconds...", NUM_THREADS, TEST_DURATION_SECONDS);

            for (int i = 0; i < NUM_THREADS; i++) {
                final int threadId = i;
                executor.submit(() -> runCacheOperations(threadId, client));
            }

            // Ensure the executor is shut down when JVM exits
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    executor.shutdownNow();

                    if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                        logger.error("Executor did not terminate in 1 second.");
                    } else {
                        logger.info("All worker threads have been shut down gracefully.");
                    }

                    CacheClient singleton = client;

                    if (singleton != null) {
                        singleton.close();
                    }

                    logger.warn("Demo terminated");
                } catch (Exception ignored) {
                }
            }));

            Thread.sleep(TimeUnit.SECONDS.toMillis(TEST_DURATION_SECONDS));

            logger.info("Test finished. Shutting down worker threads...");

            executor.shutdownNow();

            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.error("Executor did not terminate in 30 seconds.");
            } else {
                logger.info("All worker threads have been shut down gracefully.");
            }

            logger.info("Demo complete");
        }
    }

    /**
     * Simulates random cache operations (GET, PUT, DELETE) in a loop.
     *
     * @param threadId Identifier for the thread (for logging purposes).
     * @param client   The CacheClient instance to use for operations.
     */
    private static void runCacheOperations(int threadId, CacheClient client) {
        logger.info("Thread-{} started.", threadId);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                String key = "test-key-" + ThreadLocalRandom.current().nextInt(KEY_SPACE);

                int operation = ThreadLocalRandom.current().nextInt(10);

                if (operation < 5) { // GET (%50 probability)
                    String value = client.get(key);
                    logger.info("Thread-{} GET: key={}, value={}", threadId, key, value);
                } else if (operation < 9) { // PUT (%40 probability)
                    String value = UUID.randomUUID().toString();
                    client.put(key, value);
                    logger.info("Thread-{} PUT: key={}, value={}", threadId, key, value);
                } else { // DELETE (%10 probability)
                    client.delete(key);
                    logger.info("Thread-{} DELETE: key={}", threadId, key);
                }

                Thread.sleep(ThreadLocalRandom.current().nextInt(5 * 1000)); // Wait 0-5 seconds
            } catch (Exception e) {
                logger.error("Thread-{} encountered an error during cache operation", threadId, e);
            }
        }

        logger.info("Thread-{} shutting down.", threadId);
    }
}