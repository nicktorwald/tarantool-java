package org.tarantool;

public class TarantoolClientConfig {

    private String username;
    private String password;

    private int defaultRequestSize;
    private int predictedFutures;

    private int writerThreadPriority;
    private int readerThreadPriority;

    private int sharedBufferSize;
    private double directWriteFactor;

    private boolean useNewCall;

    private long initTimeoutMillis;
    private long writeTimeoutMillis;

    public static TarantoolClientConfigBuilder builder() {
        return new TarantoolClientConfigBuilder();
    }

    protected TarantoolClientConfig(TarantoolClientConfigBuilder builder) {
        this.username = builder.username;
        this.password = builder.password;
        this.defaultRequestSize = builder.defaultRequestSize;
        this.predictedFutures = builder.predictedFutures;
        this.writerThreadPriority = builder.writerThreadPriority;
        this.readerThreadPriority = builder.readerThreadPriority;
        this.sharedBufferSize = builder.sharedBufferSize;
        this.directWriteFactor = builder.directWriteFactor;
        this.useNewCall = builder.useNewCall;
        this.initTimeoutMillis = builder.initTimeoutMillis;
        this.writeTimeoutMillis = builder.writeTimeoutMillis;
    }

    /**
     * Gets an username for auth
     *
     * @return username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Gets a password for auth
     *
     * @return password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Gets a default request size when make query serialization
     *
     * @return
     */
    public int getDefaultRequestSize() {
        return defaultRequestSize;
    }

    /**
     * Gets an initial capacity for the map which holds futures of sent request
     *
     * @return initial capacity
     */
    public int getPredictedFutures() {
        return predictedFutures;
    }

    /**
     * Gets a writer thread priority
     *
     * @return thread priority
     */
    public int getWriterThreadPriority() {
        return writerThreadPriority;
    }

    /**
     * Gets a reader thread priority
     *
     * @return thread priority
     */
    public int getReaderThreadPriority() {
        return readerThreadPriority;
    }

    /**
     * Gets a shared buffer size (place where client collects requests
     * when socket is busy on write)
     *
     * @return buffer size
     */
    public int getSharedBufferSize() {
        return sharedBufferSize;
    }

    /**
     * Gets a factor to calculate a threshold whether request will be accommodated
     * in the shared buffer.
     * if request size exceeds <code>directWriteFactor * sharedBufferSize</code>
     * request is sent directly.
     *
     * @return buffer factor
     */
    public double getDirectWriteFactor() {
        return directWriteFactor;
    }

    /**
     * Gets a flag whether the client will use new CALL version.
     *
     *  Use old call command https://github.com/tarantool/doc/issues/54,
     *  please ensure that you server supports new call command
     *
     * @return flag indicating CALL command version
     */
    public boolean isUseNewCall() {
        return useNewCall;
    }

    /**
     * Gets an init timeout for synchronous operations
     *
     * @return init timeout
     */
    public long getInitTimeoutMillis() {
        return initTimeoutMillis;
    }

    /**
     * Gets a write timeout for synchronous operations
     *
     * @return write timeout
     */
    public long getWriteTimeoutMillis() {
        return writeTimeoutMillis;
    }

    public static class TarantoolClientConfigBuilder<T extends TarantoolClientConfigBuilder<T>> {

        private String username;
        private String password;
        private int defaultRequestSize = 4096;
        private int predictedFutures = (int) ((1024 * 1024) / 0.75) + 1;
        private int writerThreadPriority = Thread.NORM_PRIORITY;
        private int readerThreadPriority = Thread.NORM_PRIORITY;
        private int sharedBufferSize = 8 * 1024 * 1024;
        private double directWriteFactor = 0.5d;
        private boolean useNewCall = false;
        private long initTimeoutMillis = 60 * 1000L;
        private long writeTimeoutMillis = 60 * 1000L;

        public T setUsername(String username) {
            this.username = username;
            return self();
        }

        public T setPassword(String password) {
            this.password = password;
            return self();
        }

        public T setDefaultRequestSize(int defaultRequestSize) {
            this.defaultRequestSize = defaultRequestSize;
            return self();
        }

        public T setPredictedFutures(int predictedFutures) {
            this.predictedFutures = predictedFutures;
            return self();
        }

        public TarantoolClientConfigBuilder setWriterThreadPriority(int writerThreadPriority) {
            this.writerThreadPriority = writerThreadPriority;
            return self();
        }

        public T setReaderThreadPriority(int readerThreadPriority) {
            this.readerThreadPriority = readerThreadPriority;
            return self();
        }

        public T setSharedBufferSize(int sharedBufferSize) {
            this.sharedBufferSize = sharedBufferSize;
            return self();
        }

        public T setDirectWriteFactor(double directWriteFactor) {
            this.directWriteFactor = directWriteFactor;
            return self();
        }

        public T setUseNewCall(boolean useNewCall) {
            this.useNewCall = useNewCall;
            return self();
        }

        public T setInitTimeoutMillis(long initTimeoutMillis) {
            this.initTimeoutMillis = initTimeoutMillis;
            return self();
        }

        public T setWriteTimeoutMillis(long writeTimeoutMillis) {
            this.writeTimeoutMillis = writeTimeoutMillis;
            return self();
        }

        public TarantoolClientConfig build() {
            return new TarantoolClientConfig(this);
        }

        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }

    }

}
