package com.mehyaa.cacheclient;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test: start an in-memory ZooKeeper (Curator TestingServer), register an ephemeral
 * node under ServiceDiscovery.SERVICE_PATH and assert that ServiceDiscovery observes it.
 */
public class ServiceDiscoveryIntegrationTest {

    private TestingServer testingServer;
    private CuratorFramework curator;

    @BeforeEach
    public void setup() throws Exception {
        // start an in-memory ZooKeeper server
        testingServer = new TestingServer(true);

        // create a curator client for setup operations
        curator = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        curator.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (curator != null) {
            curator.close();
        }

        if (testingServer != null) {
            testingServer.close();
        }
    }

    @Test
    public void testServiceDiscoverySeesEphemeralNode() throws Exception {
        // ensure service path exists
        if (curator.checkExists().forPath(ServiceDiscovery.SERVICE_PATH) == null) {
            curator.create().creatingParentsIfNeeded().forPath(ServiceDiscovery.SERVICE_PATH);
        }

        // create an ephemeral sequential node with data host:port
        String payload = "127.0.0.1:8080";
        String created = curator.create().withProtection().withMode(org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(ServiceDiscovery.SERVICE_PATH + "/node-", payload.getBytes(StandardCharsets.UTF_8));

        // point ServiceDiscovery to the testing server via system property
        System.setProperty("zookeeper.connect", testingServer.getConnectString());

        ServiceDiscovery discovery = new ServiceDiscovery();

        try {
            // wait up to 5s for discovery to pick up the node
            boolean seen = false;

            for (int i = 0; i < 50; i++) {
                List<String> nodes = discovery.getNodes();

                if (nodes.contains(payload)) {
                    seen = true;
                    break;
                }

                TimeUnit.MILLISECONDS.sleep(100);
            }

            assertTrue(seen, "ServiceDiscovery should have observed the ephemeral node registration");
        } finally {
            discovery.close();
            // remove system property to avoid affecting other tests
            System.clearProperty("zookeeper.connect");
        }
    }
}
