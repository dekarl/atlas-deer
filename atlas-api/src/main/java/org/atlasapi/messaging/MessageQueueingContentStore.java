package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ForwardingContentStore;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.serialization.json.JsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import com.fasterxml.jackson.databind.ObjectMapper;


public class MessageQueueingContentStore extends ForwardingContentStore {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueingContentStore.class);

    private final JmsTemplate template;
    private final ContentStore delegate;
    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();

    public MessageQueueingContentStore(JmsTemplate template, ContentStore delegate) {
        this.template = checkNotNull(template);
        this.delegate = checkNotNull(delegate);
    }
    
    @Override
    protected ContentStore delegate() {
        return delegate;
    }

    @Override
    public <C extends Content> WriteResult<C> writeContent(C content) {
        WriteResult<C> result = super.writeContent(content);
        if (result.written()) {
            writeMessage(result);
        }
        return result;
    }

    private <C extends Content> void writeMessage(final WriteResult<C> result) {
        template.send(new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                return session.createTextMessage(serialize(createEntityUpdatedMessage(result)));
            }
        });
    }
    
    private <C extends Content> EntityUpdatedMessage createEntityUpdatedMessage(WriteResult<C> result) {
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
