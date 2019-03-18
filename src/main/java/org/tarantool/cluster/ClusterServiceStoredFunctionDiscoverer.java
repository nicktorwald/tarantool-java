package org.tarantool.cluster;

import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;
import org.tarantool.TarantoolClientOps;
import org.tarantool.TarantoolClusterClientConfig;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A cluster nodes discoverer based on calling a predefined function
 * which returns list of nodes.
 *
 * The function has to have no arguments and return list of
 * the URL-formatted strings
 */
public class ClusterServiceStoredFunctionDiscoverer implements ClusterServiceDiscoverer {

    private String infoInstance;
    private String infoFunctionName;
    private TarantoolClientConfig config;

    public ClusterServiceStoredFunctionDiscoverer(TarantoolClusterClientConfig clientConfig) {
        this.infoInstance = clientConfig.infoInstance;
        this.infoFunctionName = clientConfig.infoStoredFunction;
        this.config = clientConfig;
    }

    @Override
    public Set<String> getInstances() {
        TarantoolClientOps<Integer, List<?>, Object, List<?>> syncOperations =
                new TarantoolClientImpl(infoInstance, config).syncOps();

        List<?> list = syncOperations.call(infoFunctionName);

        List<Object> funcResult = (List<Object>) list.get(0);
        return funcResult.stream()
                .map(Object::toString)
                .map(this::parseInstanceUrl)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Gets a normalized URL.
     *
     * @param url URL string to be normalized
     * @return normalized URL
     */
    private String parseInstanceUrl(String url) {
        String[] split = url.split("@");
        if (split.length == 2) {
            return split[1];
        } else {
            return split[0];
        }
    }

}
