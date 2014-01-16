package org.atlasapi.content;

import static org.junit.Assert.assertThat;
import org.testng.annotations.Test;
import static org.hamcrest.Matchers.is;


public class ContainerSummarySerializerTest {

    private final ContainerSummarySerializer serializer = new ContainerSummarySerializer();
    
    @Test
    public void testDeSerializeContainerSummary() {
        serializeAndCheck(new Item.ContainerSummary(null, null, null, null));
        serializeAndCheck(new Item.ContainerSummary("title", null, null, null));
        serializeAndCheck(new Item.ContainerSummary(null, "desc", null, null));
        serializeAndCheck(new Item.ContainerSummary(null, null, "type", null));
        serializeAndCheck(new Item.ContainerSummary(null, null,  null, 1));
    }

    private void serializeAndCheck(Item.ContainerSummary containerSummary) {
        Item.ContainerSummary deserialized = serializer.deserialize(serializer.serialize(containerSummary));
        assertThat(deserialized.getTitle(), is(containerSummary.getTitle()));
        assertThat(deserialized.getDescription(), is(containerSummary.getDescription()));
        assertThat(deserialized.getType(), is(containerSummary.getType()));
        assertThat(deserialized.getSeriesNumber(), is(containerSummary.getSeriesNumber()));
    }

}
