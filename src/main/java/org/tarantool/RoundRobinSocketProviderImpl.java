package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Basic reconnection strategy that changes addresses in a round-robin fashion.
 * To be used with {@link TarantoolClientImpl}.
 */
public class RoundRobinSocketProviderImpl extends BaseSocketChannelProvider implements RefreshableSocketProvider {

    private static final int NO_TIMEOUT = 0;

    /**
     * Timeout to establish socket connection with an individual server.
     */
    private int timeout = NO_TIMEOUT;

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
    private ReadWriteLock addressListLock = new ReentrantReadWriteLock();

    /**
     * Constructs an instance.
     *
     * @param addresses Array of addresses in a form of [host]:[port].
     */
    public RoundRobinSocketProviderImpl(String... addresses) {
        if (addresses.length == 0) {
            throw new IllegalArgumentException("Addresses list must contain at least one address.");
        }

        updateAddressList(Arrays.asList(addresses));
    }

    private void updateAddressList(Collection<String> addresses) {
        Lock writeLock = addressListLock.writeLock();
        writeLock.lock();
        try {
            InetSocketAddress lastAddress = getLastObtainedAddress();
            socketAddresses.clear();
            addresses.stream()
                    .map(this::parseAddress)
                    .collect(Collectors.toCollection(() -> socketAddresses));
            if (lastAddress != null) {
                int recoveredPosition = socketAddresses.indexOf(lastAddress);
                currentPosition.set(recoveredPosition);
            } else {
                currentPosition.set(-1);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * @return Configured addresses in a form of [host]:[port].
     */
    public List<SocketAddress> getAddresses() {
        Lock readLock = addressListLock.readLock();
        readLock.lock();
        try {
            return Collections.unmodifiableList(this.socketAddresses);
        } finally {
            readLock.unlock();
        }
    }

    private InetSocketAddress getLastObtainedAddress() {
        Lock readLock = addressListLock.readLock();
        readLock.lock();
        try {
            int index = currentPosition.get();
            return index >= 0 ? socketAddresses.get(index) : null;
        } finally {
            readLock.unlock();
        }
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
        // todo: recalc deadline?
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
        Lock readLock = addressListLock.readLock();
        readLock.lock();
        try {
            return socketAddresses.size();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * @return Socket address to use for the next reconnection attempt.
     */
    protected InetSocketAddress getNextSocketAddress() {
        Lock readLock = addressListLock.readLock();
        readLock.lock();
        try {
            int position = currentPosition.updateAndGet(i -> (i + 1) % socketAddresses.size());
            return socketAddresses.get(position);
        } finally {
            readLock.unlock();
        }
    }

    public void refreshAddresses(Collection<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("Addresses are null or empty.");
        }
        updateAddressList(addresses);
    }
}
