package org.atlasapi.equivalence;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.content.BrandRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.JacksonMessageSerializer;
import org.atlasapi.messaging.MessageSerializer;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSource;
import com.metabroadcast.common.time.Timestamp;

public class EquivalenceGraphUpdateMessageTest {
    
  @Test
  public void testDeSerialization() throws Exception {
      MessageSerializer serializer = new JacksonMessageSerializer();
      
      EquivalenceGraphUpdateMessage egum = new EquivalenceGraphUpdateMessage("message", Timestamp.of(0), 
          ImmutableSet.of(EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC)))
      );
      
      ByteSource serialized = serializer.serialize(egum);
      
      EquivalenceGraphUpdateMessage deserialized = serializer.deserialize(serialized);
      
      assertThat(deserialized, is(egum));
      assertThat(deserialized.getUpdatedGraphs(), is(egum.getUpdatedGraphs()));
      
  }
}
