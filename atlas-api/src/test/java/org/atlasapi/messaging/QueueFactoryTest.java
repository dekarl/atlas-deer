package org.atlasapi.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.atlasapi.content.BrandRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.json.JsonFactory;
import org.testng.annotations.Test;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import com.metabroadcast.common.time.Timestamp;

public class QueueFactoryTest {

    private final class TestWorker extends BaseWorker<ResourceUpdatedMessage> {

        private final CountDownLatch latch;
        private final AtomicReference<ResourceUpdatedMessage> receiver;

        private TestWorker(CountDownLatch latch, AtomicReference<ResourceUpdatedMessage> receiver) {
            this.latch = latch;
            this.receiver = receiver;
        }

        @Override
        public void process(ResourceUpdatedMessage message) {
            receiver.set(message);
            latch.countDown();
        }
    }

    private final ConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
    private final MessageSerializer serializer = new JacksonMessageSerializer();
    private final ProducerQueueFactory pqf = new JmsProducerQueueFactory(cf, "test.system", serializer);
    private final JmsConsumerQueueFactory cqf = new JmsConsumerQueueFactory(cf, "test.system", serializer);
    
    @Test
    public void testSendingAndReceivingAMessage() throws Exception {
        String destinationName = "destination";
        MessageSender sender = pqf.makeMessageSender(destinationName);
        
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ResourceUpdatedMessage> receiver
            = new AtomicReference<ResourceUpdatedMessage>();

        Worker<ResourceUpdatedMessage> w = new TestWorker(latch, receiver);
        DefaultMessageListenerContainer container
            = cqf.makeVirtualTopicConsumer(w, "consumer", destinationName, 1, 1);
        container.initialize();
        container.start();
        
        BrandRef updated = new BrandRef(Id.valueOf(1), Publisher.BBC);
        ResourceUpdatedMessage msg = new ResourceUpdatedMessage("1", Timestamp.of(1L), updated);
        sender.sendMessage(msg);
        
        assertTrue("message not recieved", latch.await(1, TimeUnit.SECONDS));
        ResourceUpdatedMessage received = receiver.get();
        
        assertEquals(msg.getMessageId(), received.getMessageId());
        assertEquals(msg.getTimestamp(), received.getTimestamp());
        assertEquals(msg.getUpdatedResource(), received.getUpdatedResource());
        
    }
    
    @Test
    public void testSendingAndReceivingATextMessage() throws Exception {
        String destinationName = "destination1";
        
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ResourceUpdatedMessage> receiver
            = new AtomicReference<ResourceUpdatedMessage>();

        Worker<ResourceUpdatedMessage> w = new TestWorker(latch, receiver);
        DefaultMessageListenerContainer container
            = cqf.makeVirtualTopicConsumer(w, "consumer1", destinationName, 1, 1);
        container.initialize();
        container.start();

        BrandRef updated = new BrandRef(Id.valueOf(2), Publisher.BBC);
        final ResourceUpdatedMessage msg = new ResourceUpdatedMessage("2", Timestamp.of(2L), updated);
        
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

        assertTrue("message not recieved", latch.await(1, TimeUnit.SECONDS));
        ResourceUpdatedMessage received = receiver.get();
        
        assertEquals(msg.getMessageId(), received.getMessageId());
        assertEquals(msg.getTimestamp(), received.getTimestamp());
        assertEquals(msg.getUpdatedResource(), received.getUpdatedResource());
        
    }
}
