package org.tarantool.cluster;

import java.util.Set;

public interface RefreshClusterInstancesAware {

    void onInstancesRefreshed(Set<String> instances);

}
