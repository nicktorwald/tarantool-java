package org.tarantool.cluster;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Discoverer that holds static instances
 */
public class ClusterServiceListDiscoverer implements ClusterServiceDiscoverer {

    private Set<String> instances = new LinkedHashSet<>();

    public ClusterServiceListDiscoverer(Collection<String> instances) {
        this.instances.addAll(instances);
    }

    @Override
    public Set<String> getInstances() {
        return Collections.unmodifiableSet(instances);
    }

}
