package org.atlasapi.messaging;

import javax.jms.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.adapter.MessageListenerAdapter;

public final class JmsConsumerQueueFactory {
    
    public final class ContainerBuilder<M extends Message> {
        
        private final Worker<M> worker;
        
        private final String producer;
        private final String consumer;
        
        private String producerSystem = system;
        private String consumerSystem = system;
        private MessageSerializer serializer = JmsConsumerQueueFactory.this.serializer;
        
        private int consumerDefltCount = 1;
        private int consumerMaxCount = 1;


        private ContainerBuilder(Worker<M> worker, String producer, String consumer) {
            this.worker = worker;
            this.producer = producer;
            this.consumer = consumer;
        }
        
        public ContainerBuilder<M> withProducerSystem(String producerSystem) {
            this.producerSystem = producerSystem;
            return this;
        }
        
        public ContainerBuilder<M> withConsumerSystem(String consumerSystem) {
            this.consumerSystem = consumerSystem;
            return this;
        }
        
        public ContainerBuilder<M> withSerializer(MessageSerializer serializer) {
            this.serializer = serializer;
            return this;
        }
        
        public ContainerBuilder<M> withDefaultConsumers(int defltConsumers) {
            this.consumerDefltCount = defltConsumers;
            return this;
        }

        public ContainerBuilder<M> withMaxConsumers(int maxConsumers) {
            this.consumerMaxCount = maxConsumers;
            return this;
        }
        
        public DefaultMessageListenerContainer build() {
            String destination = virtualTopicConsumer(consumerSystem, consumer, producerSystem, producer);
            return makeContainer(worker, serializer, destination, consumerDefltCount, consumerMaxCount);
        }
        
        private String virtualTopicConsumer(String consumerSystem, String consumer, String producerSystem, String producer) {
            consumerSystem = consumerSystem.replace(".", "");
            return String.format("Consumer.%s%s.VirtualTopic.%s.%s", consumerSystem, consumer, producerSystem, producer);
        }
        
    }

    private static final Logger log = LoggerFactory.getLogger(JmsConsumerQueueFactory.class);
    
    private final String system;
    private final ConnectionFactory cf;
    private final MessageSerializer serializer;

    public JmsConsumerQueueFactory(ConnectionFactory cf, String system, MessageSerializer serializer) {
        this.cf = cf;
        this.system = system;
        this.serializer = serializer;
    }
    
    public <M extends Message> DefaultMessageListenerContainer makeQueueConsumer(Worker<M> worker, String name, int consumers, int maxConsumers) {
        return makeContainer(worker, serializer, String.format("%s.%s", name, system), consumers, maxConsumers);
    }
    
    public <M extends Message> ContainerBuilder<M> makeVirtualTopicConsumer(Worker<M> worker, String producer, String consumer) {
        return new ContainerBuilder<M>(worker, consumer, producer);
    }
    
//
//    public <M extends Message> DefaultMessageListenerContainer makeVirtualTopicConsumer(Worker<M> worker, 
//            MessageSerializer serializer, String consumerSystem, String consumer, String producerSystem, String producer, int consumers, int maxConsumers) {
//        String consumerName = virtualTopicConsumer(consumerSystem, consumer, producerSystem, producer);
//        DefaultMessageListenerContainer container = makeContainer(worker, serializer, consumerName, consumers, maxConsumers);
//        return container;
//    }
//    
    
    private <M extends Message> DefaultMessageListenerContainer makeContainer(Worker<M> worker, MessageSerializer serializer, 
            String destination, int consumers, int maxConsumers) {
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
