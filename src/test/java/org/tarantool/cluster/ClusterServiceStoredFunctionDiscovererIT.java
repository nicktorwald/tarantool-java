package org.tarantool.cluster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.tarantool.AbstractTarantoolConnectorIT;
import org.tarantool.TarantoolClusterClientConfig;
import org.tarantool.TarantoolControl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestUtils.makeDiscoveryFunction;
import static org.tarantool.TestUtils.makeInstanceEnv;

class ClusterServiceStoredFunctionDiscovererIT {

    protected static final int INSTANCE_LISTEN_PORT = 3301;
    protected static final int INSTANCE_ADMIN_PORT = 3313;
    private static final String LUA_FILE = "jdk-testing.lua";

    private static final String INSTANCE_NAME = "jdk-testing";
    private static TarantoolControl control;
    private static TarantoolClusterClientConfig clusterConfig;

    private static String INFO_FUNCTION_NAME = "getAddresses";


    @BeforeAll
    public static void setupEnv() {
        control = new TarantoolControl();
        control.createInstance(INSTANCE_NAME, LUA_FILE, makeInstanceEnv(INSTANCE_LISTEN_PORT, INSTANCE_ADMIN_PORT));

        control.start(INSTANCE_NAME);
        control.waitStarted(INSTANCE_NAME);

        clusterConfig = AbstractTarantoolConnectorIT.makeClusterClientConfig();
        clusterConfig.infoInstance = "localhost:" + INSTANCE_LISTEN_PORT;
        clusterConfig.infoStoredFunction = INFO_FUNCTION_NAME;
    }

    @AfterAll
    public static void tearDownEnv() {
        control.stop(INSTANCE_NAME);
        control.waitStopped(INSTANCE_NAME);
    }

    @Test
    @DisplayName("Discoverer successfully fetches and parses a node list from an info node.")
    void testSuccessfulAddressParsing() {
        List<String> addresses = Arrays.asList("localhost:3311", "127.0.0.1:3301");
        String functionCode = makeDiscoveryFunction(INFO_FUNCTION_NAME, addresses);
        control.openConsole(INSTANCE_NAME).exec(functionCode);

        ClusterServiceStoredFunctionDiscoverer discoverer =
                new ClusterServiceStoredFunctionDiscoverer(clusterConfig);

        Set<String> instances = discoverer.getInstances();

        assertNotNull(instances);
        assertEquals(2, instances.size());
        assertTrue(instances.contains(addresses.get(0)));
        assertTrue(instances.contains(addresses.get(1)));
    }

    @Test
    @DisplayName("Discoverer successfully fetches and parses a node list with extra auth prefixes.")
    void testSuccessfulAuthAddressParsing() {
        List<String> addresses = Arrays.asList("localhost:3311", "127.0.0.1:3301", "127.0.0.2:3301");
        List<String> authAddresses = Arrays.asList("admin:test@" + addresses.get(0), addresses.get(1), "root:12345@" + addresses.get(2));

        String functionCode = makeDiscoveryFunction(INFO_FUNCTION_NAME, authAddresses);
        control.openConsole(INSTANCE_NAME).exec(functionCode);

        ClusterServiceStoredFunctionDiscoverer discoverer =
                new ClusterServiceStoredFunctionDiscoverer(clusterConfig);

        Set<String> instances = discoverer.getInstances();

        assertNotNull(instances);
        assertEquals(3, instances.size());
        assertTrue(instances.contains(addresses.get(0)));
        assertTrue(instances.contains(addresses.get(1)));
        assertTrue(instances.contains(addresses.get(2)));
    }

    @Test
    @DisplayName("Discoverer successfully fetches and parses a node list with duplicates.")
    void testSuccessfulUniqueAddressParsing() {
        List<String> addresses = Arrays.asList("localhost:3311", "127.0.0.1:3301", "127.0.0.2:3301", "localhost:3311");

        String functionCode = makeDiscoveryFunction(INFO_FUNCTION_NAME, addresses);
        control.openConsole(INSTANCE_NAME).exec(functionCode);

        ClusterServiceStoredFunctionDiscoverer discoverer =
                new ClusterServiceStoredFunctionDiscoverer(clusterConfig);

        Set<String> instances = discoverer.getInstances();

        assertNotNull(instances);
        assertEquals(3, instances.size());
        assertTrue(instances.contains(addresses.get(0)));
        assertTrue(instances.contains(addresses.get(1)));
        assertTrue(instances.contains(addresses.get(3)));
    }


    @Test
    @DisplayName("Gracefully process case when the function returns empty node list")
    void testFunctionReturnedEmptyList() {
        String functionCode = makeDiscoveryFunction(INFO_FUNCTION_NAME, Collections.emptyList());
        control.openConsole(INSTANCE_NAME).exec(functionCode);

        ClusterServiceStoredFunctionDiscoverer discoverer =
                new ClusterServiceStoredFunctionDiscoverer(clusterConfig);


        Set<String> instances = discoverer.getInstances();

        assertNotNull(instances);
        assertTrue(instances.isEmpty());
    }

}