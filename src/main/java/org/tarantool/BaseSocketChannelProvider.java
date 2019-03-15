package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public abstract class BaseSocketChannelProvider implements SocketChannelProvider {

    /**
     * Limit of retries.
     */
    private int retriesLimit = RETRY_NO_LIMIT;

    @Override
    public final SocketChannel get(int retryNumber, Throwable lastError) {
        if (areRetriesExhausted(retryNumber)) {
            throw new CommunicationException("Connection retries exceeded.", lastError);
        }

        return doRetry(retryNumber, lastError);
    }

    protected abstract SocketChannel doRetry(int retryNumber, Throwable lastError);

    /**
     * Sets maximum amount of reconnect attempts to be made before an exception is raised.
     * The retry count is maintained by a {@link #get(int, Throwable)} caller
     * when a socket level connection was established.
     *
     * Negative value means unlimited attempts.
     *
     * @param retriesLimit Limit of retries to use.
     */
    public void setRetriesLimit(int retriesLimit) {
        this.retriesLimit = retriesLimit;
    }

    /**
     * @return Maximum reconnect attempts to make before raising exception.
     */
    public int getRetriesLimit() {
        return retriesLimit;
    }

    /**
     * Parse a string address in the form of [host]:[port]
     * and builds a socket address.
     *
     * @param address Server address.
     * @return Socket address.
     */
    protected InetSocketAddress parseAddress(String address) {
        int idx = address.indexOf(':');
        String host = (idx < 0) ? address : address.substring(0, idx);
        int port = (idx < 0) ? 3301 : Integer.parseInt(address.substring(idx + 1));
        return new InetSocketAddress(host, port);
    }

    protected SocketChannel openChannel(InetSocketAddress socketAddress, int timeout) throws IOException {
        SocketChannel channel = null;
        try {
            channel = SocketChannel.open();
            channel.socket().connect(socketAddress, timeout);
            return channel;
        } catch (IOException e) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignored) {
                    // No-op.
                }
            }
            throw e;
        }
    }

    /**
     * Provides a decision on whether retries limit is hit.
     *
     * @param retries Current count of retries.
     * @return {@code true} if retries are exhausted.
     */
    private boolean areRetriesExhausted(int retries) {
        int limit = getRetriesLimit();
        if (limit < 0)
            return false;
        return retries >= limit;
    }
}
