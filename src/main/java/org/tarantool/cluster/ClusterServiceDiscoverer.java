package org.tarantool.cluster;

import java.util.Set;

public interface ClusterServiceDiscoverer {

    Set<String> getInstances();

}
