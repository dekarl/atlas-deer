package org.atlasapi.messaging;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.common.joda.time.DateTime;
import org.hamcrest.Matchers;
import org.junit.Test;

import com.google.common.base.Charsets;


public class AbstractWorkerTest {
    
    private final MessageSerializer serializer = new JacksonMessageSerializer();

    @Test
    public void testOnMessage() throws Exception {
        final AtomicBoolean processed = new AtomicBoolean(false);
        AbstractWorker worker = new AbstractWorker(serializer) {
            
            @Override
            public void process(EntityUpdatedMessage message) {
                assertThat(message.getEntityId(), Matchers.is("hello"));
                processed.set(true);
            }
            
        };
        
        worker.onMessage(new String(serializer.serialize(message("hello")).read(), Charsets.UTF_8));
        assertTrue(processed.get());
    }

    private EntityUpdatedMessage message(String msg) {
        return new EntityUpdatedMessage("1", new DateTime().getMillis(), msg, "type", "src");
    }

}
