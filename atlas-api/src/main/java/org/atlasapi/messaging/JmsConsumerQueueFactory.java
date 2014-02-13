package org.atlasapi.messaging;

import javax.jms.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.adapter.MessageListenerAdapter;

public final class JmsConsumerQueueFactory {

    private static final Logger log = LoggerFactory.getLogger(JmsConsumerQueueFactory.class);
    
    private final String system;
    private final ConnectionFactory cf;
    private final MessageSerializer serializer;

    public JmsConsumerQueueFactory(ConnectionFactory cf, String system, MessageSerializer serializer) {
        this.cf = cf;
        this.system = system;
        this.serializer = serializer;
    }

    private String virtualTopicConsumer(String consumer, String producerSystem, String producer) {
        return String.format("Consumer.%s.VirtualTopic.%s.%s", consumer, producerSystem, producer);
    }
    
    private String queueName(String name) {
        return String.format("%s.%s", name, system);
    }
    
    public <M extends Message> DefaultMessageListenerContainer makeVirtualTopicConsumer(Worker<M> worker, String consumer, String producer, int consumers, int maxConsumers) {
        return makeVirtualTopicConsumer(worker, serializer, consumer, system, producer, consumers, maxConsumers);
    }

    public <M extends Message> DefaultMessageListenerContainer makeVirtualTopicConsumer(Worker<M> worker, 
            MessageSerializer serializer, String consumer, String producerSystem, String producer, int consumers, int maxConsumers) {
        String consumerName = virtualTopicConsumer(consumer, producerSystem, producer);
        DefaultMessageListenerContainer container = makeContainer(worker, serializer, consumerName, consumers, maxConsumers);
        return container;
    }

    public <M extends Message> DefaultMessageListenerContainer makeQueueConsumer(Worker<M> worker, String name, int consumers, int maxConsumers) {
        String queueName = queueName(name);
        return makeContainer(worker, serializer, queueName, consumers, maxConsumers);
    }
    
    private <M extends Message> DefaultMessageListenerContainer makeContainer(Worker<M> worker, MessageSerializer serializer, String destination, int consumers, int maxConsumers) {
        log.info("Reading {} with {}", destination, worker.getType().getSimpleName());
        
        JmsMessageAdapter<M> messageAdapter = new JmsMessageAdapter<M>(serializer, worker);
        MessageListenerAdapter adapter = new MessageListenerAdapter(messageAdapter);
        adapter.setDefaultListenerMethod("onMessage");

        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
        container.setMessageListener(adapter);
        container.setConnectionFactory(cf);
        container.setDestinationName(destination);
        container.setConcurrentConsumers(consumers);
        container.setMaxConcurrentConsumers(maxConsumers);
        
        return container;
    }
   
    
}
