package com.mehyaa.cacheclient;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CacheClient.class);

    private static volatile CacheClient INSTANCE;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ServiceDiscovery discovery;
    private final OkHttpClient http;
    private final ConsistentHash hashRing;

    private CacheClient() throws Exception {
        discovery = new ServiceDiscovery();

        List<String> nodes = discovery.getNodes();

        hashRing = new ConsistentHash(nodes);

        discovery.addChangeListener((oldNodes, newNodes) -> refreshRing(oldNodes, newNodes));

        http = new OkHttpClient.Builder()
                .connectTimeout(2, TimeUnit.SECONDS)
                .callTimeout(5, TimeUnit.SECONDS)
                .build();

        // Ensure the singleton is closed when JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                CacheClient singleton = INSTANCE;

                if (singleton != null) {
                    singleton.close();
                }
            } catch (Exception ignored) {
            }
        }));
    }

    /**
     * Returns the singleton instance, lazily creating it if necessary.
     */
    public static CacheClient getInstance() throws Exception {
        CacheClient result = INSTANCE;

        if (result == null) {
            synchronized (CacheClient.class) {
                result = INSTANCE;

                if (result == null) {
                    INSTANCE = result = new CacheClient();
                }
            }
        }

        return result;
    }

    /**
     * Gets the value for the given key, or null if not found.
     */
    public String get(String key) throws IOException {
        try (Response response = sendRequest("GET", key, null)) {
            if (response.isSuccessful()) {
                return response.body().string();
            }

            if (response.code() == 404) {
                return null;
            }

            throw new IOException("GET failed with code " + response.code());
        }
    }

    /**
     * Sets the value for the given key.
     */
    public void put(String key, String value) throws IOException {
        try (Response response = sendRequest("POST", key, value)) {
            if (!response.isSuccessful()) {
                throw new IOException("PUT failed with code " + response.code());
            }
        }
    }

    /**
     * Deletes the given key.
     */
    public void delete(String key) throws IOException {
        try (Response response = sendRequest("DELETE", key, null)) {
            if (!response.isSuccessful() && response.code() != 404) {
                throw new IOException("DELETE failed with code " + response.code());
            }
        }
    }

    /**
     * Closes the cache client and releases resources.
     */
    @Override
    public synchronized void close() throws Exception {
        // Make close idempotent
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        // Close discovery first to stop receiving further updates
        try {
            discovery.close();
        } catch (Exception e) {
            logger.warn("Error closing discovery: {}", e.getMessage(), e);
        }

        // Shutdown OkHttp executor and wait a short time for tasks to finish
        try {
            ExecutorService exec = http.dispatcher().executorService();
            exec.shutdown();

            if (!exec.awaitTermination(5, TimeUnit.SECONDS)) {
                exec.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.warn("Error shutting down HTTP executor: {}", e.getMessage(), e);
        }

        try {
            http.connectionPool().evictAll();
        } catch (Exception e) {
            logger.warn("Error evicting connection pool: {}", e.getMessage(), e);
        }

        try {
            if (http.cache() != null) {
                http.cache().close();
            }
        } catch (Exception e) {
            logger.warn("Error closing HTTP cache: {}", e.getMessage(), e);
        }

        // Null out the global reference so a new instance can be created if needed
        if (INSTANCE == this) {
            INSTANCE = null;
        }
    }

    private synchronized void refreshRing(List<String> oldNodes, List<String> newNodes) {
        if (hashRing == null) {
            return;
        }

        // Normalize nulls to empty sets to avoid NullPointerException in later removeAll
        Set<String> oldSet = oldNodes == null ? Collections.emptySet() : new HashSet<>(oldNodes);
        Set<String> newSet = newNodes == null ? Collections.emptySet() : new HashSet<>(newNodes);

        // compute additions and removals
        Set<String> toAdd = new HashSet<>(newSet);
        toAdd.removeAll(oldSet);

        Set<String> toRemove = new HashSet<>(oldSet);
        toRemove.removeAll(newSet);

        // Removals first
        for (String node : toRemove) {
            try {
                hashRing.remove(node);
            } catch (Exception e) {
                logger.warn("Error removing node from hash ring: {}", e.getMessage(), e);
            }
        }

        // Additions
        for (String node : toAdd) {
            try {
                hashRing.add(node);
            } catch (Exception e) {
                logger.warn("Error adding node to hash ring: {}", e.getMessage(), e);
            }
        }
    }

    private String chooseNode(String key) {
        return hashRing.get(key);
    }

    private Response sendRequest(String method, String key, String body) throws IOException {
        String node = chooseNode(key);

        if (node == null) {
            throw new IOException("No cache nodes available");
        }

        String url = String.format("http://%s/%s", node, key);
        Request.Builder requestBuilder = new Request.Builder().url(url);

        switch (method) {
            case "GET":
                requestBuilder.get();
                break;

            case "POST":
                RequestBody requestBody = body != null ? RequestBody.create(body, MediaType.get("text/plain")) : null;
                requestBuilder.post(requestBody);
                break;

            case "DELETE":
                requestBuilder.delete();
                break;

            default:
                throw new IllegalArgumentException("Unknown method " + method);
        }

        Request request = requestBuilder.build();

        return http.newCall(request).execute();
    }
}