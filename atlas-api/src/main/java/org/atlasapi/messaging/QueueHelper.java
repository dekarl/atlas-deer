package org.atlasapi.messaging;

import javax.jms.ConnectionFactory;

import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.adapter.MessageListenerAdapter;

public final class QueueHelper {

    private final String system;
    private final ConnectionFactory cf;

    public QueueHelper(ConnectionFactory cf, String system) {
        this.cf = cf;
        this.system = system;
    }

    private String virtualTopicProducer(String name) {
        return String.format("VirtualTopic.%s.%s", system, name);
    }
    
    private String virtualTopicConsumer(String consumer, String producer) {
        return String.format("Consumer.%s.VirtualTopic.%s.%s", consumer, system, producer);
    }
    
    private String replayDestination(String name) {
        return String.format("%s.Replay.%s", name, system);
    }
    
    public DefaultMessageListenerContainer makeVirtualTopicConsumer(Worker worker, String consumer, String producer, int consumers, int maxConsumers) {
        return makeContainer(worker, virtualTopicConsumer(consumer, producer), consumers, maxConsumers);
    }

    public DefaultMessageListenerContainer makeReplayContainer(Worker worker, String name, int consumers, int maxConsumers) {
        return makeContainer(worker, replayDestination(name), consumers, maxConsumers);
    }
    
    public DefaultMessageListenerContainer makeContainer(Worker worker, String destination, int consumers, int maxConsumers) {
        MessageListenerAdapter adapter = new MessageListenerAdapter(worker);
        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();

        adapter.setDefaultListenerMethod("onMessage");
        container.setConnectionFactory(cf);
        container.setDestinationName(destination);
        container.setConcurrentConsumers(consumers);
        container.setMaxConcurrentConsumers(maxConsumers);
        container.setMessageListener(adapter);

        return container;
    }
    
    public JmsTemplate makeVirtualTopicProducer(String producerName) {
        JmsTemplate jmsTemplate = new JmsTemplate(cf);
        jmsTemplate.setPubSubDomain(true);
        jmsTemplate.setDefaultDestinationName(this.virtualTopicProducer(producerName));
        return jmsTemplate;
    }
    
}
