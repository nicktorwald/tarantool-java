package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * Simple provider that produces a single connection
 * To be used with {@link TarantoolClientImpl}.
 */
public class SingleSocketChannelProviderImpl extends BaseSocketChannelProvider {

    private InetSocketAddress address;
    private int timeout;

    /**
     *
     *
     * @param address instance address
     * @param timeout time in millis
     */
    public SingleSocketChannelProviderImpl(String address, int timeout) {
        this.address = parseAddress(address);
        this.timeout = timeout;
    }

    @Override
    protected SocketChannel doRetry(int retryNumber, Throwable lastError) {
        long deadline = System.currentTimeMillis();
        try {
            return openChannel(address, timeout);
        } catch (IOException e) {
            if (deadline <= System.currentTimeMillis()) {
                throw new CommunicationException("Connection time out.", e);
            }
            throw new CommunicationException("Connection error.", e);
        }
    }

}
