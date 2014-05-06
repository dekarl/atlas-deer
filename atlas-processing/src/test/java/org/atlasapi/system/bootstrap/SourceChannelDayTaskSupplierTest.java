package org.atlasapi.system.bootstrap;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.LocalDate;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DayRangeGenerator;
import com.metabroadcast.common.time.TimeMachine;

@RunWith(MockitoJUnitRunner.class)
public class SourceChannelDayTaskSupplierTest {

    @Mock private SourceChannelDayFactory<Integer> factory;
    @Mock private ChannelResolver channelResolver;
    
    private DayRangeGenerator dayRangeGenerator = new DayRangeGenerator()
        .withLookAhead(1)
        .withLookBack(0);
    private Set<Publisher> srcs = ImmutableSet.of(Publisher.BBC, Publisher.PA);
    private Clock clock = new TimeMachine();

    private SourceChannelDayTaskSupplier<Integer> supplier;
    
    @Before
    public void setup() {
        supplier = new SourceChannelDayTaskSupplier<Integer>(factory, channelResolver, dayRangeGenerator, srcs, clock);
    }
    
    @Test
    public void testSuppliesTasksForAllSrcDayChannels() {
        
        Channel channel1 = Channel.builder().withUri("channel1").build();
        Channel channel2 = Channel.builder().withUri("channel2").build();
        
        when(channelResolver.all()).thenReturn(ImmutableList.of(channel1, channel2));
        when(factory.create(any(Publisher.class), any(Channel.class), any(LocalDate.class)))
            .thenReturn(1);
        
        ImmutableList<Integer> numbers = ImmutableList.copyOf(supplier.get());
        
        assertThat(numbers.size(), is(8));
        
        verify(factory).create(Publisher.BBC, channel1, clock.now().toLocalDate());
        verify(factory).create(Publisher.BBC, channel1, clock.now().toLocalDate().plusDays(1));
        verify(factory).create(Publisher.BBC, channel2, clock.now().toLocalDate());
        verify(factory).create(Publisher.BBC, channel2, clock.now().toLocalDate().plusDays(1));
        verify(factory).create(Publisher.PA, channel1, clock.now().toLocalDate());
        verify(factory).create(Publisher.PA, channel1, clock.now().toLocalDate().plusDays(1));
        verify(factory).create(Publisher.PA, channel2, clock.now().toLocalDate());
        verify(factory).create(Publisher.PA, channel2, clock.now().toLocalDate().plusDays(1));
        
    }

}
