package org.tarantool;

import java.util.concurrent.Executor;

/**
 * Configuration for the {@link TarantoolClusterClient}.
 */
public class TarantoolClusterClientConfig extends TarantoolClientConfig {

    private int operationExpiryTimeMillis;
    private Executor executor;
    private String infoInstance;
    private String infoStoredFunction;
    private int infoScanTimeMillis;

    /**
     * Gets a period for the operation is eligible for retry.
     *
     * @return expiry period
     */
    public int getOperationExpiryTimeMillis() {
        return operationExpiryTimeMillis;
    }

    /**
     * Gets an executor that will be used as a thread of execution to retry writes.
     *
     * @return executor
     */
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Gets an instance address which contains {@link TarantoolClusterClientConfig#infoStoredFunction}
     * returning a pool of extra instances.
     *
     * @return info instance
     */
    public String getInfoInstance() {
        return infoInstance;
    }

    /**
     * Gets a name of the stored function to be used
     * to fetch list of instances.
     *
     * @return stored function name
     */
    public String getInfoStoredFunction() {
        return infoStoredFunction;
    }

    /**
     * Gets a scan period for refreshing a new list of instances.
     *
     * @return expiry period
     */
    public int getInfoScanTimeMillis() {
        return infoScanTimeMillis;
    }

    public static TarantoolClusterClientConfigBuilder builder() {
        return new TarantoolClusterClientConfigBuilder();
    }

    private TarantoolClusterClientConfig(TarantoolClusterClientConfigBuilder builder) {
        super(builder);
        this.operationExpiryTimeMillis = builder.operationExpiryTimeMillis;
        this.executor = builder.executor;
        this.infoInstance = builder.infoInstance;
        this.infoStoredFunction = builder.infoStoredFunction;
        this.infoScanTimeMillis = builder.infoScanTimeMillis;
    }

    public static class TarantoolClusterClientConfigBuilder extends TarantoolClientConfigBuilder<TarantoolClusterClientConfigBuilder> {

        private int operationExpiryTimeMillis = 500;
        private Executor executor;
        private String infoInstance;
        private String infoStoredFunction;
        private int infoScanTimeMillis = 60_000;

        public TarantoolClusterClientConfigBuilder setOperationExpiryTimeMillis(int operationExpiryTimeMillis) {
            this.operationExpiryTimeMillis = operationExpiryTimeMillis;
            return self();
        }

        public TarantoolClusterClientConfigBuilder setExecutor(Executor executor) {
            this.executor = executor;
            return self();
        }

        public TarantoolClusterClientConfigBuilder setInfoInstance(String infoInstance) {
            this.infoInstance = infoInstance;
            return self();
        }

        public TarantoolClusterClientConfigBuilder setInfoStoredFunction(String infoStoredFunction) {
            this.infoStoredFunction = infoStoredFunction;
            return self();
        }

        public TarantoolClusterClientConfigBuilder setInfoScanPeriod(int infoScanTimeMillis) {
            this.infoScanTimeMillis = infoScanTimeMillis;
            return self();
        }

        public TarantoolClusterClientConfig build() {
            return new TarantoolClusterClientConfig(this);
        }
    }

}
