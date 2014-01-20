package org.atlasapi;

import org.atlasapi.content.CassandraContentStore;
import org.atlasapi.content.CassandraEquivalentContentStore;
import org.atlasapi.content.ContentHasher;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.equivalence.CassandraEquivalenceGraphStore;
import org.atlasapi.equivalence.CassandraEquivalenceRecordStore;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.messaging.ProducerQueueFactory;
import org.atlasapi.schedule.CassandraScheduleStore;
import org.atlasapi.topic.CassandraTopicStore;
import org.atlasapi.topic.Topic;

import com.datastax.driver.core.Session;
import com.google.common.base.Equivalence;
import com.google.common.util.concurrent.AbstractIdleService;
import com.metabroadcast.common.ids.IdGeneratorBuilder;
import com.metabroadcast.common.properties.Configurer;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ConsistencyLevel;


public class CassandraPersistenceModule extends AbstractIdleService implements PersistenceModule {

    private String contentEquivalenceGraphChanges = Configurer.get("messaging.destination.equivalence.content.graph.changes").get();
    
    private final String keyspace;

    private final AstyanaxContext<Keyspace> context;
    private final CassandraContentStore contentStore;
    private final CassandraTopicStore topicStore;
    private final CassandraScheduleStore scheduleStore;
    private final CassandraEquivalenceRecordStore equivalenceRecordStore;
    private final DatastaxCassandraService dataStaxService;

    private CassandraEquivalenceGraphStore contentEquivalenceGraphStore;
    private CassandraEquivalentContentStore equivalentContentStore;

    private ProducerQueueFactory messageSenderFactory;
    
    public CassandraPersistenceModule(ProducerQueueFactory messageSenderFactory, 
            AstyanaxContext<Keyspace> context, DatastaxCassandraService datastaxCassandraService, 
            String keyspace, IdGeneratorBuilder idGeneratorBuilder, ContentHasher hasher) {
        this.messageSenderFactory = messageSenderFactory;
        this.keyspace = keyspace;
        this.context = context;
        this.contentStore = CassandraContentStore.builder(context, "content", 
            hasher, idGeneratorBuilder.generator("content"))
            .withReadConsistency(ConsistencyLevel.CL_ONE)
            .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
            .build();
        this.topicStore = CassandraTopicStore.builder(context, "topics", 
            topicEquivalence(), idGeneratorBuilder.generator("topic"))
            .withReadConsistency(ConsistencyLevel.CL_ONE)
            .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
            .build();
        this.scheduleStore = CassandraScheduleStore.builder(context, "schedule", contentStore)
                .withReadConsistency(ConsistencyLevel.CL_ONE)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();
        this.equivalenceRecordStore = new CassandraEquivalenceRecordStore(
            context, "equivalence_record", ConsistencyLevel.CL_ONE, ConsistencyLevel.CL_QUORUM
        );
        this.dataStaxService = datastaxCassandraService;
    }

    @Override
    protected void startUp() throws Exception {
        Session session = dataStaxService.getSession(keyspace);
        com.datastax.driver.core.ConsistencyLevel read = com.datastax.driver.core.ConsistencyLevel.ONE;
        com.datastax.driver.core.ConsistencyLevel write = com.datastax.driver.core.ConsistencyLevel.QUORUM;
        this.contentEquivalenceGraphStore = new CassandraEquivalenceGraphStore(messageSenderFactory.makeMessageSender(contentEquivalenceGraphChanges), session, read, write);
        this.equivalentContentStore = new CassandraEquivalentContentStore(contentStore, contentEquivalenceGraphStore, session, read, write);
    }

    @Override
    protected void shutDown() throws Exception {
        context.shutdown();
    }
    
    public AstyanaxContext<Keyspace> getContext() {
        return this.context;
    }
    
    @Override
    public CassandraContentStore contentStore() {
        return contentStore; 
    }

    @Override
    public CassandraTopicStore topicStore() {
        return topicStore;
    }
    
    @Override
    public CassandraScheduleStore scheduleStore() {
        return this.scheduleStore;
    }
    
    private Equivalence<? super Topic> topicEquivalence() {
        return new Equivalence<Topic>(){

            @Override
            protected boolean doEquivalent(Topic a, Topic b) {
                return false;
            }

            @Override
            protected int doHash(Topic t) {
                return 0;
            }
        };
    }
    
    public CassandraEquivalenceRecordStore getEquivalenceRecordStore() {
        return this.equivalenceRecordStore;
    }

    public EquivalenceGraphStore contentEquivalenceGraphStore() {
        return this.contentEquivalenceGraphStore;
    }

    public EquivalentContentStore equivalentContentStore() {
        return this.equivalentContentStore;
    }
    
}
