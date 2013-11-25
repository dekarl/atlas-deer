package org.atlasapi.query.v4.schedule;

import static org.mockito.Mockito.mock;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.persistence.output.ContainerSummaryResolver;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.schedule.ChannelSchedule;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;
import com.metabroadcast.common.time.DateTimeZones;

public class ScheduleQueryResultWriterTest {

    private final ContainerSummaryResolver containerSummaryResolver = mock(ContainerSummaryResolver.class);
    private final AnnotationRegistry<Content> contentAnnotations = AnnotationRegistry.<Content>builder().build();
    private final AnnotationRegistry<Channel> channelAnnotations = AnnotationRegistry.<Channel>builder().build();
    private final EntityListWriter<ChannelSchedule> scheduleWriter
        = new ScheduleListWriter(new ChannelListWriter(channelAnnotations), new ContentListWriter(contentAnnotations));
    private final ScheduleQueryResultWriter writer = new ScheduleQueryResultWriter(scheduleWriter);
      
    
    @Test
    public void testWrite() throws IOException {
        Channel channel = Channel.builder().build();
        channel.setId(1234l);
        
        DateTime from = new DateTime(0, DateTimeZones.UTC);
        DateTime to = new DateTime(1, DateTimeZones.UTC);
        Interval interval = new Interval(from, to);
        
        Item item = new Item("aUri","aCurie",Publisher.BBC);
        item.setId(4321l);
        item.setTitle("aTitle");
        
        Broadcast broadcast = new Broadcast(channel.getCanonicalUri(), from, to);
        ItemAndBroadcast itemAndBroadcast = new ItemAndBroadcast(item, broadcast );
        Iterable<ItemAndBroadcast> entries = ImmutableList.of(itemAndBroadcast);
        ChannelSchedule cs = new ChannelSchedule(channel, interval, entries);
        
        HttpServletRequest request = new StubHttpServletRequest();
        StubHttpServletResponse response = new StubHttpServletResponse();
        JsonResponseWriter responseWriter = new JsonResponseWriter(request, response);
        QueryContext context = QueryContext.standard();
        QueryResult<ChannelSchedule> result = QueryResult.singleResult(cs, context);
        
        writer.write(result, responseWriter);
        
        response.getWriter().flush();
        System.out.println(response.getResponseAsString());
    }

}
