package org.tarantool;

import java.util.concurrent.Executor;

/**
 * Configuration for the {@link TarantoolClusterClient}.
 */
public class TarantoolClusterClientConfig extends TarantoolClientConfig {

    /**
     * Period for the operation is eligible for retry.
     */
    public int operationExpiryTimeMillis = 500;

    /**
     * Executor that will be used as a thread of
     * execution to retry writes.
     */
    public Executor executor;

    /**
     * Gets an instance address which contain {@link TarantoolClusterClientConfig#infoStoredFunction}
     * returning a pool of extra instances.
     */
    public String infoInstance;

    /**
     * Gets a name of the stored function to be used
     * to fetch list of instances.
     */
    public String infoStoredFunction;

    /**
     * Scan period for refreshing a new list of instances.
     */
    public int infoScanTimeMillis = 60_000;

}
