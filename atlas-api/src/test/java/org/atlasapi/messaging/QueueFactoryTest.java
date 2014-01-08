package org.atlasapi.messaging;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.atlasapi.serialization.json.JsonFactory;
import org.junit.Test;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

public class QueueFactoryTest {

    private final ConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
    private final MessageSerializer serializer = new JacksonMessageSerializer();
    private final ProducerQueueFactory pqf = new ProducerQueueFactory(cf, "test.system", serializer);
    private final ConsumerQueueFactory cqf = new ConsumerQueueFactory(cf, "test.system", serializer);
    
    @Test
    public void testSendingAndReceivingAMessage() throws Exception {
        String destinationName = "destination";
        MessageSender sender = pqf.makeMessageSender(destinationName);
        
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<EntityUpdatedMessage> reciever
            = new AtomicReference<EntityUpdatedMessage>();

        Worker w = new BaseWorker() {
            @Override
            public void process(EntityUpdatedMessage message) {
                reciever.set(message);
                latch.countDown();
            }
        };
        DefaultMessageListenerContainer container
            = cqf.makeVirtualTopicConsumer(w, "consumer", destinationName, 1, 1);
        container.initialize();
        container.start();
        
        Message msg = new EntityUpdatedMessage("1", 1L, "eid", "etype", "esrc");
        sender.sendMessage(msg);
        
        latch.await();
        EntityUpdatedMessage received = reciever.get();
        
        assertEquals(msg.getMessageId(), received.getMessageId());
        assertEquals(msg.getTimestamp(), received.getTimestamp());
        assertEquals(msg.getEntityId(), received.getEntityId());
        assertEquals(msg.getEntityType(), received.getEntityType());
        assertEquals(msg.getEntitySource(), received.getEntitySource());
        
    }
    
    @Test
    public void testSendingAndReceivingATextMessage() throws Exception {
        String destinationName = "destination1";
        
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<EntityUpdatedMessage> reciever
            = new AtomicReference<EntityUpdatedMessage>();

        Worker w = new BaseWorker() {
            @Override
            public void process(EntityUpdatedMessage message) {
                reciever.set(message);
                latch.countDown();
            }
        };
        DefaultMessageListenerContainer container
            = cqf.makeVirtualTopicConsumer(w, "consumer1", destinationName, 1, 1);
        container.initialize();
        container.start();
        
        final Message msg = new EntityUpdatedMessage("2", 2L, "fid", "ftype", "fsrc");
        
        JmsTemplate template = new JmsTemplate(cf);
        template.setPubSubDomain(true);
        template.setDefaultDestinationName("VirtualTopic.test.system."+destinationName);
        template.send(new MessageCreator() {
            @Override
            public javax.jms.Message createMessage(Session session) throws JMSException {
                try {
                    return session.createTextMessage(JsonFactory.makeJsonMapper().writeValueAsString(msg));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        
        latch.await();
        EntityUpdatedMessage received = reciever.get();
        
        assertEquals(msg.getMessageId(), received.getMessageId());
        assertEquals(msg.getTimestamp(), received.getTimestamp());
        assertEquals(msg.getEntityId(), received.getEntityId());
        assertEquals(msg.getEntityType(), received.getEntityType());
        assertEquals(msg.getEntitySource(), received.getEntitySource());
        
    }
}
