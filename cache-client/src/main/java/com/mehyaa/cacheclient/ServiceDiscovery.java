package com.mehyaa.cacheclient;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple service discovery that watches a ZooKeeper path for child nodes
 * containing host:port strings.
 */
public class ServiceDiscovery {
    public static final String SERVICE_PATH = "/cache/nodes";

    private static final Logger logger = LoggerFactory.getLogger(ServiceDiscovery.class);

    private final CopyOnWriteArrayList<String> nodes = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<BiConsumer<List<String>, List<String>>> listeners = new CopyOnWriteArrayList<>();

    private final CuratorFramework zkClient;
    private final CuratorCache zkCache;


    public ServiceDiscovery() throws Exception {
        zkClient = CuratorFrameworkFactory.newClient(getZkConnectString(), new ExponentialBackoffRetry(1000, 3));
        zkClient.start();

        zkCache = CuratorCache.build(zkClient, SERVICE_PATH);

        CuratorCacheListener listener = CuratorCacheListener.builder()
                .forAll((type, oldData, newData) -> {
                    try {
                        rebuildNodes();
                    } catch (Exception e) {
                        // swallow to avoid breaking the cache listener
                        logger.warn("Error rebuilding nodes from CuratorCache event: {}", e.getMessage(), e);
                    }
                })
                .build();

        zkCache.listenable().addListener(listener);
        zkCache.start();

        rebuildNodes();
    }

    /**
     * Returns a snapshot of currently known nodes.
     */
    public List<String> getNodes() {
        return new ArrayList<>(nodes);
    }

    /**
     * Register a listener that will be called when the node list changes.
     */
    public void addChangeListener(BiConsumer<List<String>, List<String>> listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    /**
     * Closes the service discovery and releases resources.
     */
    public void close() throws Exception {
        zkCache.close();
        zkClient.close();
    }

    /**
     * Reads ZooKeeper connect string from application variables
     * (system property 'zookeeper.connect' or env 'ZOOKEEPER_CONNECT' / 'ZK_CONNECT')
     * or falls back to localhost:2181.
     */
    private static String getZkConnectString() {
        String property = System.getProperty("zookeeper.connect");

        if (property != null && !property.isEmpty()) {
            return property;
        }

        String environment = System.getenv("ZOOKEEPER_CONNECT");

        if (environment != null && !environment.isEmpty()) {
            return environment;
        }

        environment = System.getenv("ZK_CONNECT");

        if (environment != null && !environment.isEmpty()) {
            return environment;
        }

        // fallback to localhost
        return "localhost:2181";
    }

    /**
     * Rebuilds the internal node list from ZooKeeper and notifies listeners.
     */
    private void rebuildNodes() {
        List<String> newNodes = new ArrayList<>();

        try {
            List<String> children = zkClient.getChildren().forPath(SERVICE_PATH);

            for (String child : children) {
                byte[] data = zkClient.getData().forPath(SERVICE_PATH + "/" + child);

                if (data != null && data.length > 0) {
                    newNodes.add(new String(data, StandardCharsets.UTF_8));
                }
            }
        } catch (KeeperException.NoNodeException ignored) {
            // The service path doesn't exist yet; treat as no nodes registered.
        } catch (Exception e) {
            // For other errors, log and propagate as a runtime exception so callers
            // aware of severe failures. Listener callbacks already guard against
            // exceptions.
            throw new RuntimeException(e);
        }

        List<String> oldNodes = new ArrayList<>(nodes);

        nodes.clear();

        if (!newNodes.isEmpty()) {
            nodes.addAll(newNodes);
        }

        for (BiConsumer<List<String>, List<String>> listener : listeners) {
            try {
                listener.accept(oldNodes, newNodes);
            } catch (Exception e) {
                // swallow listener exceptions to not break discovery
                logger.warn("ServiceDiscovery listener threw: {}", e.getMessage(), e);
            }
        }
    }
}