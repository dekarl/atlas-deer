package org.atlasapi.topic;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.MessageSender;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Equivalence;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.Timestamp;


public abstract class AbstractTopicStore implements TopicStore {

    private final IdGenerator idGenerator;
    private final Equivalence<? super Topic> equivalence;
    private final MessageSender sender;
    private final Clock clock;

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public AbstractTopicStore(IdGenerator idGenerator, Equivalence<? super Topic> equivalence, MessageSender sender, Clock clock) {
        this.idGenerator = checkNotNull(idGenerator);
        this.equivalence = checkNotNull(equivalence);
        this.sender = checkNotNull(sender);
        this.clock = checkNotNull(clock);
    }
    
    @Override
    public WriteResult<Topic> writeTopic(Topic topic) {
        checkNotNull(topic, "write null topic");
        checkNotNull(topic.getPublisher(), "write unsourced topic");
        
        Topic previous = getPreviousTopic(topic);
        if (previous != null) {
            if (equivalence.equivalent(topic, previous)) {
                return WriteResult.unwritten(topic)
                    .withPrevious(previous)
                    .build();
            }
            topic.setId(previous.getId());
            topic.setFirstSeen(previous.getFirstSeen());
        }
        
        DateTime now = clock.now();
        if (topic.getFirstSeen() == null) {
            topic.setFirstSeen(now);
        }
        topic.setLastUpdated(now);
        doWrite(ensureId(topic), previous);
        WriteResult<Topic> result = WriteResult.written(topic)
                .withPrevious(previous)
                .build();
        if (result.written()) {
            writeMessage(result);
        }
        return result;
    }
    
    private void writeMessage(final WriteResult<Topic> result) {
        ResourceUpdatedMessage message = createEntityUpdatedMessage(result);
        try {
            sender.sendMessage(message);
        } catch (Exception e) {
            log.error(message.getUpdatedResource().toString(), e);
        }
    }

    private <T extends Topic> ResourceUpdatedMessage createEntityUpdatedMessage(
            WriteResult<T> result) {
        return new ResourceUpdatedMessage(
                UUID.randomUUID().toString(),
                Timestamp.of(result.getWriteTime().getMillis()),
                result.getResource().toRef());
    }

    private Topic ensureId(Topic topic) {
        topic.setId(topic.getId() != null ? topic.getId()
                                          : Id.valueOf(idGenerator.generateRaw()));
        return topic;
    }

    protected abstract void doWrite(Topic topic, Topic previous);

    private Topic getPreviousTopic(Topic topic) {
        return resolvePrevious(topic.getId(), topic.getPublisher(), topic.getAliases());
    }
    
    @Nullable
    protected abstract Topic resolvePrevious(@Nullable Id id, Publisher source, Set<Alias> aliases);
    
}
