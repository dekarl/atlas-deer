package org.atlasapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.Session;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.AbstractIdleService;

public class DatastaxCassandraService extends AbstractIdleService {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Iterable<String> nodes;

    private Cluster cluster;

    public DatastaxCassandraService(Iterable<String> nodes) {
        this.nodes = nodes;
    }

    @Override
    protected void startUp() throws Exception {
        connect();
    }

    private void connect() {
        log.info("connecting to   nodes: {}", Joiner.on(", ").join(nodes));
        this.cluster = Cluster.builder()
                .addContactPoints(FluentIterable.from(nodes).toArray(String.class))
                .withCompression(Compression.SNAPPY)
                .build();
        Metadata metadata = cluster.getMetadata();
        log.info("connected  to cluster: {}", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            log.debug("datacenter: {}; host: {}; rack: {}",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
    }

    public Session getSession(String keyspace) {
        return cluster.connect(keyspace);
    }

    public Cluster getCluster() {
        return cluster;
    }

    @Override
    protected void shutDown() throws Exception {
        log.info("disconnecting");
        cluster.shutdown();
    }
}
