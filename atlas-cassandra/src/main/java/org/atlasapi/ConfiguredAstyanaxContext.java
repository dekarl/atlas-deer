package org.atlasapi;

import java.util.concurrent.Executors;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;


public class ConfiguredAstyanaxContext implements Supplier<AstyanaxContext<Keyspace>>{
    
    private final String cluster;
    private final String keyspace;
    private final Iterable<String> seeds;
    private final int port;
    private final int clientThreads;
    private final int connectionTimeout;
    
    public ConfiguredAstyanaxContext(String cluster, String keyspace, Iterable<String> seeds,
            int port, int clientThreads, int connectionTimeout) {
        this.cluster = cluster;
        this.keyspace = keyspace;
        this.seeds = seeds;
        this.port = port;
        this.clientThreads = clientThreads;
        this.connectionTimeout = connectionTimeout;
    }

    public AstyanaxContext<Keyspace> get() {
        return new AstyanaxContext.Builder()
            .forCluster(cluster)
            .forKeyspace(keyspace)
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                .setAsyncExecutor(Executors.newFixedThreadPool(
                    clientThreads,
                    new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat("astyanax-%d")
                        .build()
                ))
            )
            .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("altas")
                .setSeeds(Joiner.on(",").join(seeds))
                .setPort(port)
                .setConnectTimeout(connectionTimeout)
                .setMaxBlockedThreadsPerHost(clientThreads)
                .setMaxConnsPerHost(clientThreads)
                .setMaxConns(clientThreads * 5)
            )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());
    }
    
}
