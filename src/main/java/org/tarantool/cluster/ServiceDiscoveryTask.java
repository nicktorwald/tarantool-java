package org.tarantool.cluster;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 *
 */
public class ServiceDiscoveryTask implements Runnable {

    private ClusterServiceDiscoverer serviceDiscoverer;
    private Collection<RefreshClusterInstancesAware> subscribers;

    private Set<String> lastInstances;

    public ServiceDiscoveryTask(ClusterServiceDiscoverer serviceDiscoverer, Collection<RefreshClusterInstancesAware> subscribers) {
        this.serviceDiscoverer = serviceDiscoverer;
        this.subscribers = subscribers;
    }

    @Override
    public void run() {
        try {
            Set<String> freshInstances = serviceDiscoverer.getInstances();
            if (!Objects.equals(lastInstances, freshInstances)) {
                lastInstances = freshInstances;
                subscribers.forEach(subscriber -> subscriber.onInstancesRefreshed(lastInstances));
            }
        } catch (Exception ignored) {
        }
    }

}
