package org.atlasapi;

import org.atlasapi.content.CassandraContentStore;
import org.atlasapi.content.CassandraEquivalentContentStore;
import org.atlasapi.content.ContentHasher;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.equivalence.CassandraEquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.messaging.JacksonMessageSerializer;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.schedule.CassandraEquivalentScheduleStore;
import org.atlasapi.schedule.CassandraScheduleStore;
import org.atlasapi.schedule.EquivalentScheduleStore;
import org.atlasapi.schedule.ScheduleUpdateMessage;
import org.atlasapi.topic.CassandraTopicStore;
import org.atlasapi.topic.Topic;

import com.datastax.driver.core.Session;
import com.google.common.base.Equivalence;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.AbstractIdleService;
import com.metabroadcast.common.ids.IdGeneratorBuilder;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSenderFactory;
import com.metabroadcast.common.time.SystemClock;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ConsistencyLevel;

public class CassandraPersistenceModule extends AbstractIdleService implements PersistenceModule {

    private String contentEquivalenceGraphChanges = Configurer.get("messaging.destination.equivalence.content.graph.changes").get();
    private String contentChanges = Configurer.get("messaging.destination.content.changes").get();
    private String topicChanges = Configurer.get("messaging.destination.topics.changes").get();
    private String scheduleChanges = Configurer.get("messaging.destination.schedule.changes").get();
    
    private Boolean processing = Objects.firstNonNull(Configurer.get("processing.config"), Parameter.valueOf("false")).toBoolean();
    
    private final String keyspace;

    private final AstyanaxContext<Keyspace> context;
    private final CassandraContentStore contentStore;
    private final CassandraTopicStore topicStore;
    private final CassandraScheduleStore scheduleStore;
    private final DatastaxCassandraService dataStaxService;

    private CassandraEquivalenceGraphStore contentEquivalenceGraphStore;
    private CassandraEquivalentContentStore equivalentContentStore;
    private CassandraEquivalentScheduleStore equivalentScheduleStore;

    private MessageSenderFactory messageSenderFactory;
    
    public CassandraPersistenceModule(MessageSenderFactory messageSenderFactory, 
            AstyanaxContext<Keyspace> context, DatastaxCassandraService datastaxCassandraService, 
            String keyspace, IdGeneratorBuilder idGeneratorBuilder, ContentHasher hasher) {
        this.messageSenderFactory = messageSenderFactory;
        this.keyspace = keyspace;
        this.context = context;
        ConsistencyLevel readConsistency = processing ? ConsistencyLevel.CL_QUORUM : ConsistencyLevel.CL_ONE;
        this.contentStore = CassandraContentStore.builder(context, "content", 
            hasher, sender(contentChanges, ResourceUpdatedMessage.class), idGeneratorBuilder.generator("content"))
            .withReadConsistency(readConsistency)
            .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
            .build();
        this.topicStore = CassandraTopicStore.builder(context, "topics", 
            topicEquivalence(), sender(topicChanges, ResourceUpdatedMessage.class), idGeneratorBuilder.generator("topic"))
            .withReadConsistency(readConsistency)
            .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
            .build();
        this.scheduleStore = CassandraScheduleStore.builder(context, "schedule", contentStore, sender(scheduleChanges, ScheduleUpdateMessage.class))
                .withReadConsistency(readConsistency)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();
        this.dataStaxService = datastaxCassandraService;
    }

    @Override
    protected void startUp() throws Exception {
        dataStaxService.awaitRunning();
        Session session = dataStaxService.getSession(keyspace);
        com.datastax.driver.core.ConsistencyLevel read = processing ? com.datastax.driver.core.ConsistencyLevel.QUORUM
                                                                    : com.datastax.driver.core.ConsistencyLevel.ONE;
        com.datastax.driver.core.ConsistencyLevel write = com.datastax.driver.core.ConsistencyLevel.QUORUM;
        this.contentEquivalenceGraphStore = new CassandraEquivalenceGraphStore(sender(contentEquivalenceGraphChanges, EquivalenceGraphUpdateMessage.class), session, read, write);
        this.equivalentContentStore = new CassandraEquivalentContentStore(contentStore, contentEquivalenceGraphStore, session, read, write);
        this.equivalentScheduleStore = new CassandraEquivalentScheduleStore(contentEquivalenceGraphStore, contentStore, session, read, write, new SystemClock());
    }

    private <M extends Message> MessageSender<M> sender(String dest, Class<M> type) {
        return messageSenderFactory.makeMessageSender(dest, JacksonMessageSerializer.forType(type));
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

    public EquivalenceGraphStore contentEquivalenceGraphStore() {
        return this.contentEquivalenceGraphStore;
    }

    public EquivalentContentStore equivalentContentStore() {
        return this.equivalentContentStore;
    }
    
    public EquivalentScheduleStore equivalentScheduleStore() {
        return this.equivalentScheduleStore;
    }
    
}
