package com.mehyaa.sample.cacheclient;

import com.mehyaa.cacheclient.CacheClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleCacheClientApplication {

    private static final Logger logger = LoggerFactory.getLogger(SampleCacheClientApplication.class);

    public static void main(String[] args) throws Exception {
        CacheClient client = null;

        try {
            client = CacheClient.getInstance();

            String key = "hello";
            String value = "world";

            logger.info("Putting {}={} into cache", key, value);
            client.put(key, value);

            logger.info("Getting {} from cache", key);
            String read = client.get(key);

            logger.info("Read value: {}", read);

            logger.info("Deleting {}", key);
            client.delete(key);

            logger.info("Demo complete");
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception ignored) {
                }
            }
        }
    }
}