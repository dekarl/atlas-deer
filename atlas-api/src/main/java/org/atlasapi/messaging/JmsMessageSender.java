package org.atlasapi.messaging;

import java.io.IOException;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Session;

import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

public class JmsMessageSender implements MessageSender {

    private final JmsTemplate template;
    private final MessageSerializer serializer;

    public JmsMessageSender(JmsTemplate template, MessageSerializer serializer) {
        this.template = template;
        this.serializer = serializer;
    }

    @Override
    public void sendMessage(final Message message) throws IOException {
        final byte[] bytes = serialize(message);
        template.send(new MessageCreator() {
            @Override
            public javax.jms.Message createMessage(Session session) throws JMSException {
                BytesMessage bytesMessage = session.createBytesMessage();
                bytesMessage.writeBytes(bytes);
                return bytesMessage;
            }
        });
    }
    
    private byte[] serialize(final Message message) throws IOException {
            return serializer.serialize(message).read();
    }
    
}
