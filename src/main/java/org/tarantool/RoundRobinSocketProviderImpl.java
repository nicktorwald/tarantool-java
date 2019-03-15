package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic reconnection strategy that changes addresses in a round-robin fashion.
 * To be used with {@link TarantoolClientImpl}.
 */
public class RoundRobinSocketProviderImpl extends BaseSocketChannelProvider {

    /**
     * Timeout to establish socket connection with an individual server.
     */
    private int timeout; // 0 is infinite.

    /**
     * Server addresses as configured.
     */
    private final List<String> addresses = new ArrayList<>();

    /**
     * Socket addresses.
     */
    private final List<InetSocketAddress> socketAddresses = new ArrayList<>();

    /**
     * Current position within {@link #socketAddresses} array.
     */
    private AtomicInteger currentPosition = new AtomicInteger(-1);

    /**
     * Lock
     */
//    private ReadWriteLock addressListLock = new ReentrantReadWriteLock();

    /**
     * Constructs an instance.
     *
     * @param addresses Array of addresses in a form of [host]:[port].
     */
    public RoundRobinSocketProviderImpl(String... addresses) {
        if (addresses == null || addresses.length == 0) {
            throw new IllegalArgumentException("Addresses are null or empty.");
        }

        updateAddressList(Arrays.asList(addresses));
    }

    private void updateAddressList(Collection<String> addresses) {
        String lastAddress = getLastObtainedAddress();
        this.addresses.clear();
        this.addresses.addAll(addresses);
        this.addresses.forEach(address -> socketAddresses.add(parseAddress(address)));
        if (lastAddress != null) {
            int recoveredPosition = this.addresses.indexOf(lastAddress);
            currentPosition.set(recoveredPosition);
        }
    }

    /**
     * @return Configured addresses in a form of [host]:[port].
     */
    public List<String> getAddresses() {
        return Collections.unmodifiableList(this.addresses);
    }

    public String getLastObtainedAddress() {
        int index = currentPosition.get();
        return index >= 0 ? addresses.get(index) : null;
    }

    /**
     * Sets maximum amount of time to wait for a socket connection establishment
     * with an individual server.
     * <p>
     * Zero means infinite timeout.
     *
     * @param timeout Timeout value, ms.
     * @return {@code this}.
     * @throws IllegalArgumentException If timeout is negative.
     */
    public RoundRobinSocketProviderImpl setTimeout(int timeout) {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout is negative.");
        }

        this.timeout = timeout;

        return this;
    }

    /**
     * @return Maximum amount of time to wait for a socket connection establishment
     * with an individual server.
     */
    public int getTimeout() {
        return timeout;
    }

    @Override
    protected SocketChannel doRetry(int retryNumber, Throwable lastError) {
        int attempts = getAddressCount();
        long deadline = System.currentTimeMillis() + timeout * attempts;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                return openChannel(getNextSocketAddress(), timeout);
            } catch (IOException e) {
                long now = System.currentTimeMillis();
                if (deadline <= now) {
                    throw new CommunicationException("Connection time out.", e);
                }
                if (--attempts == 0) {
                    // Tried all addresses without any lack, but still have time.
                    attempts = getAddressCount();
                    try {
                        Thread.sleep((deadline - now) / attempts);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        throw new CommunicationException("Thread interrupted.", new InterruptedException());
    }

    /**
     * @return Number of configured addresses.
     */
    protected int getAddressCount() {
        return socketAddresses.size();
    }

    /**
     * @return Socket address to use for the next reconnection attempt.
     */
    protected InetSocketAddress getNextSocketAddress() {
        int position = currentPosition.updateAndGet(i -> (i + 1) % socketAddresses.size());
        return socketAddresses.get(position);
    }

    public void setAddresses(Collection<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("Addresses are null or empty.");
        }
        updateAddressList(addresses);
    }
}
