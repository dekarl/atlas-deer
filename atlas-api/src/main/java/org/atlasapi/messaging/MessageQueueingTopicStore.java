package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.atlasapi.topic.ForwardingTopicStore;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicStore;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.serialization.json.JsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageQueueingTopicStore extends ForwardingTopicStore {
    
    private static final Logger log = LoggerFactory.getLogger(MessageQueueingContentStore.class);

    private final JmsTemplate template;
    private final TopicStore delegate;
    
    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();

    public MessageQueueingTopicStore(JmsTemplate template, TopicStore delegate) {
        this.template = checkNotNull(template);
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
        template.send(new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                return session.createTextMessage(serialize(createEntityUpdatedMessage(result)));
            }
        });
    }
    
    private <T extends Topic> EntityUpdatedMessage createEntityUpdatedMessage(WriteResult<T> result) {
        return new EntityUpdatedMessage(
                UUID.randomUUID().toString(),
                result.getWriteTime().getMillis(),
                result.getResource().getId().toString(),
                result.getClass().getSimpleName().toLowerCase(),
                result.getResource().getPublisher().key());
    }
    
    private String serialize(final EntityUpdatedMessage message) {
        String result = null;
        try {
            result = mapper.writeValueAsString(message);
        } catch (Exception e) {
            log.error(message.getEntityId(), e);
        }
        return result;
    }
}
