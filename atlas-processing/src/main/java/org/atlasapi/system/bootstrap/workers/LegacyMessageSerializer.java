package org.atlasapi.system.bootstrap.workers;

import java.util.concurrent.atomic.AtomicReference;

import org.atlasapi.messaging.EntityUpdatedMessage;
import org.atlasapi.messaging.Message;
import org.atlasapi.messaging.MessageException;
import org.atlasapi.messaging.MessageSerializer;
import org.atlasapi.messaging.worker.v3.Worker;
import org.atlasapi.serialization.json.JsonFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteSource;


public class LegacyMessageSerializer implements MessageSerializer {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    
    @Override
    public ByteSource serialize(Message msg) throws MessageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Message deserialize(ByteSource bytes) throws MessageException {
        try {
            org.atlasapi.messaging.v3.Message legacy = mapper.readValue(bytes.read(), org.atlasapi.messaging.v3.Message.class);
            final AtomicReference<Message> message = new AtomicReference<>();
            legacy.dispatchTo(new Worker(){

                @Override
                public void process(org.atlasapi.messaging.v3.EntityUpdatedMessage leg) {
                    message.set(new EntityUpdatedMessage(
                        leg.getMessageId(), 
                        leg.getTimestamp(), 
                        leg.getEntityId(), 
                        leg.getEntityType(), 
                        leg.getEntitySource()
                    ));
                }

                @Override
                public void process(org.atlasapi.messaging.v3.BeginReplayMessage leg) {
                    
                }

                @Override
                public void process(org.atlasapi.messaging.v3.EndReplayMessage leg) {
                    
                }

                @Override
                public void process(org.atlasapi.messaging.v3.ReplayMessage leg) {
                    
                }
            });
            return message.get();
        } catch (Exception e) {
            throw new MessageException(e);
        }
    }

}
