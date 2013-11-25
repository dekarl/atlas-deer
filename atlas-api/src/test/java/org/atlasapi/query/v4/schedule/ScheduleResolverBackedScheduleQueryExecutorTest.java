package org.atlasapi.query.v4.schedule;

import static org.atlasapi.media.entity.Publisher.METABROADCAST;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import javax.annotation.Nullable;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.equiv.MergingEquivalentsResolver;
import org.atlasapi.equiv.ResolvedEquivalents;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.Schedule;
import org.atlasapi.schedule.ScheduleResolver;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.DateTimeZones;

@RunWith(MockitoJUnitRunner.class)
public class ScheduleResolverBackedScheduleQueryExecutorTest {

    @SuppressWarnings("unchecked")
    private final MergingEquivalentsResolver<Content> equivalentContentResolver = mock(MergingEquivalentsResolver.class); 
    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
    private final ScheduleResolver scheduleResolver = mock(ScheduleResolver.class);
    
    private final ScheduleResolverBackedScheduleQueryExecutor executor
            = new ScheduleResolverBackedScheduleQueryExecutor(channelResolver, scheduleResolver, equivalentContentResolver);
    
    @Test
    public void testExecutingScheduleQuery() throws Exception {
        
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        channel.setCanonicalUri("one");
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        ScheduleQuery query = ScheduleQuery.single(METABROADCAST, interval, QueryContext.standard(), Id.valueOf(channel.getId()));

        ChannelSchedule channelSchedule = new ChannelSchedule(channel, interval, ImmutableList.<ItemAndBroadcast>of());

        when(channelResolver.fromId(channel.getId()))
            .thenReturn(Maybe.just(channel));
        when(scheduleResolver.resolve(argThat(hasItems(channel)), eq(interval), eq(query.getSource())))
                .thenReturn(Futures.immediateFuture(new Schedule(ImmutableList.of(channelSchedule), interval)));
        
        QueryResult<ChannelSchedule> result = executor.execute(query);
        
        assertThat(result.getOnlyResource(), is(channelSchedule));
    }
    
    @Test
    public void testThrowsExceptionIfChannelIsMissing() {
        
        when(channelResolver.fromId(any(Long.class)))
            .thenReturn(Maybe.<Channel>nothing());

        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        ScheduleQuery query = ScheduleQuery.single(METABROADCAST, interval, QueryContext.standard(), Id.valueOf(1));
        
        try {
            executor.execute(query);
            fail("expected NotFoundException");
        } catch (QueryExecutionException qee) {
            assertThat(qee, is(instanceOf(NotFoundException.class)));
            verifyZeroInteractions(scheduleResolver);
        }
    }

    @Test
    public void testResolvesEquivalentContentForApiKeyWithPrecedenceEnabled() throws Exception {
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        channel.setCanonicalUri("one");
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        List<SourceReadEntry> reads = ImmutableList.copyOf(Iterables.transform(Publisher.all(), new Function<Publisher, SourceReadEntry>() {
           @Override
            public SourceReadEntry apply(@Nullable Publisher input) {
                return new SourceReadEntry(input, SourceStatus.AVAILABLE_ENABLED);
            }}));
        
        ApplicationSources appSources = ApplicationSources.defaults().copy()
                .withPrecedence(true)
                .withReadableSources(reads)
                .build();
        QueryContext context = new QueryContext(appSources, ActiveAnnotations.standard());
        
        Id itemId = Id.valueOf(1);
        ChannelSchedule channelSchedule = new ChannelSchedule(channel, interval, ImmutableList.<ItemAndBroadcast>of(
            new ItemAndBroadcast(
                new Item(itemId, METABROADCAST), 
                new Broadcast(channel.getCanonicalUri(), interval)
            )
        ));

        ScheduleQuery query = ScheduleQuery.single(METABROADCAST, interval, context, Id.valueOf(channel.getId()));

        Item equivalentItem = new Item(itemId, METABROADCAST);
        when(channelResolver.fromId(channel.getId()))
            .thenReturn(Maybe.just(channel));
        when(scheduleResolver.resolve(argThat(hasItems(channel)), eq(interval), eq(query.getSource())))
            .thenReturn(Futures.immediateFuture(new Schedule(ImmutableList.of(channelSchedule), interval)));
        when(equivalentContentResolver.resolveIds(ImmutableList.of(itemId), appSources, ActiveAnnotations.standard().all()))
            .thenReturn(Futures.immediateFuture(ResolvedEquivalents.<Content>builder().putEquivalents(itemId, ImmutableList.of(equivalentItem)).build()));
        
        QueryResult<ChannelSchedule> result = executor.execute(query);
        
        assertThat(result.getOnlyResource().getEntries().get(0).getItem(), sameInstance(equivalentItem));
        verify(equivalentContentResolver).resolveIds(ImmutableList.of(itemId), appSources, ActiveAnnotations.standard().all());
        
    }
    
}
