package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.topic.ForwardingTopicStore;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.time.Timestamp;

public class MessageQueueingTopicStore extends ForwardingTopicStore {
    
    private static final Logger log = LoggerFactory.getLogger(MessageQueueingContentStore.class);

    private final MessageSender sender;
    private final TopicStore delegate;
    
    public MessageQueueingTopicStore(MessageSender sender, TopicStore delegate) {
        this.sender = checkNotNull(sender);
        this.delegate = checkNotNull(delegate);
    }
    
    @Override
    protected TopicStore delegate() {
        return delegate;
    }

    @Override
    public WriteResult<Topic> writeTopic(Topic topic) {
        WriteResult<Topic> result = delegate.writeTopic(topic);
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
    
    private <T extends Topic> ResourceUpdatedMessage createEntityUpdatedMessage(WriteResult<T> result) {
        return new ResourceUpdatedMessage(
                UUID.randomUUID().toString(),
                Timestamp.of(result.getWriteTime().getMillis()),
                result.getResource().toRef());
    }
}
