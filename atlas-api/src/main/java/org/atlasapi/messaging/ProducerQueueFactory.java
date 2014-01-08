package org.atlasapi.messaging;

import javax.jms.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;

public class ProducerQueueFactory {

    private static final Logger log = LoggerFactory.getLogger(ProducerQueueFactory.class);

    private final String system;
    private final ConnectionFactory cf;
    private final MessageSerializer serializer;

    public ProducerQueueFactory(ConnectionFactory cf, String system, MessageSerializer serializer) {
        this.cf = cf;
        this.system = system;
        this.serializer = serializer;
    }

    public MessageSender makeMessageSender(String destinationName) {
        String destination = virtualTopicProducer(destinationName);
        log.info("Writing {}", destination);
        JmsTemplate jmsTemplate = new JmsTemplate(cf);
        jmsTemplate.setPubSubDomain(true);
        jmsTemplate.setDefaultDestinationName(destination);
        return new JmsMessageSender(jmsTemplate, serializer);
    }

    private String virtualTopicProducer(String name) {
        return String.format("VirtualTopic.%s.%s", system, name);
    }

}
