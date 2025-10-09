package com.mehyaa.cacheserver;

import com.mehyaa.cacheserver.cache.InMemoryCache;

import io.javalin.Javalin;
import io.javalin.http.Handler;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Enumeration;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheServerApplication {
    private static final int DEFAULT_PORT = 6379;

    private static final Logger logger = LoggerFactory.getLogger(CacheServerApplication.class);

    public static void main(String[] args) {
        final int port = parsePort(args);

        InMemoryCache cache = InMemoryCache.getInstance();

        // Start Javalin
        Javalin app = Javalin.create().start(port);

        AtomicReference<CuratorFramework> zkClientRef = new AtomicReference<>();
        AtomicReference<String> registeredPathRef = new AtomicReference<>();

        // Register with ZooKeeper if configured
        String zkConnectString = getZkConnectString();

        if (zkConnectString != null && !zkConnectString.isEmpty()) {
            try {
                CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
                        new ExponentialBackoffRetry(1000, 3));
                zkClient.start();
                zkClientRef.set(zkClient);

                String host = detectHostAddress();
                String data = host + ":" + port;
                String path = "/cache/nodes/node-" + host + "-" + port;
                String registeredPath = zkClient.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(path, data.getBytes(StandardCharsets.UTF_8));
                registeredPathRef.set(registeredPath);
                logger.info("Registered in ZooKeeper at {} -> {}", registeredPath, data);
            } catch (Exception e) {
                logger.warn("Failed to register in ZooKeeper: {}", e.getMessage(), e);
            }
        }

        // Register handlers (use functions to create handlers bound to the cache)
        app.get("/*", createGetHandler(cache));
        app.put("/*", createUpsertHandler(cache));
        app.post("/*", createUpsertHandler(cache));
        app.delete("/*", createDeleteHandler(cache));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop();

            logger.info("Cache server stopped.");

            CuratorFramework curator = zkClientRef.get();

            if (curator != null) {
                try {
                    String registeredPath = registeredPathRef.get();

                    if (registeredPath != null) {
                        curator.delete().forPath(registeredPath);
                    }
                } catch (Exception ignored) {
                }

                curator.close();
            }
        }));

        logger.info("Cache server started on port {}", port);
    }

    /**
     * Parses the port number from command line arguments.
     * Falls back to DEFAULT_PORT if not specified or invalid.
     */
    private static int parsePort(String[] args) {
        if (args.length > 0) {
            try {
                int port = Integer.parseInt(args[0]);

                if (port > 1000 && port < 65536) {
                    return port;
                }

                logger.warn("Invalid port number {}. Using default port {}", args[0], DEFAULT_PORT);
            } catch (NumberFormatException e) {
                logger.warn("Invalid port format {}. Using default port {}", args[0], DEFAULT_PORT);
            }
        }

        return DEFAULT_PORT;
    }

    /**
     * Creates a handler for GET requests.
     */
    private static Handler createGetHandler(InMemoryCache cache) {
        return ctx -> {
            String rawPath = ctx.path();
            String key = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;

            if (key.isEmpty()) {
                ctx.status(400);
                return;
            }

            String value = cache.get(key);

            if (value != null) {
                ctx.contentType("text/plain");
                ctx.result(value).status(200);
            } else {
                ctx.status(404);
            }
        };
    }

    /**
     * Creates a handler for PUT/POST requests.
     */
    private static Handler createUpsertHandler(InMemoryCache cache) {
        return ctx -> {
            String rawPath = ctx.path();
            String key = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;

            if (key.isEmpty()) {
                ctx.status(400);
                return;
            }

            String body = ctx.body();

            if (body == null || body.isEmpty()) {
                ctx.status(400);
                return;
            }

            cache.put(key, body);

            ctx.status(200);
        };
    }

    /**
     * Creates a handler for DELETE requests.
     */
    private static Handler createDeleteHandler(InMemoryCache cache) {
        return ctx -> {
            String rawPath = ctx.path();
            String key = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;

            if (key.isEmpty()) {
                ctx.status(400);
                return;
            }

            cache.delete(key);

            ctx.status(200);
        };
    }

    /**
     * Reads ZooKeeper connect string from application variables
     * (system property 'zookeeper.connect' or env 'ZOOKEEPER_CONNECT' /
     * 'ZK_CONNECT')
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
     * Attempts to detect a non-loopback IPv4 address of the host.
     * Falls back to 127.0.0.1 if no suitable address is found.
     */
    private static String detectHostAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

            while (interfaces.hasMoreElements()) {
                NetworkInterface nInterface = interfaces.nextElement();

                if (!nInterface.isUp() || nInterface.isLoopback() || nInterface.isVirtual()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = nInterface.getInetAddresses();

                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();

                    if (!address.isLoopbackAddress() && address instanceof Inet4Address) {
                        return address.getHostAddress();
                    }
                }
            }
        } catch (Exception e) {
            // ignore
        }

        return "127.0.0.1";
    }
}