package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicWriter;
import org.atlasapi.messaging.AbstractWorker;
import org.atlasapi.messaging.EntityUpdatedMessage;
import org.atlasapi.persistence.topic.TopicQueryResolver;

import com.metabroadcast.common.base.Maybe;


public class TopicReadWriter extends AbstractWorker {

    private final TopicQueryResolver reader;
    private final TopicWriter writer;

    public TopicReadWriter(TopicQueryResolver reader, TopicWriter writer) {
        this.reader = checkNotNull(reader);
        this.writer = checkNotNull(writer);
    }
    
    @Override
    public void process(EntityUpdatedMessage message) {
        Maybe<org.atlasapi.media.entity.Topic> read = reader.topicForId(Long.valueOf(message.getEntityId()));
        if (read.hasValue()) {
            writer.writeTopic(read.requireValue());
        }
    }

}
