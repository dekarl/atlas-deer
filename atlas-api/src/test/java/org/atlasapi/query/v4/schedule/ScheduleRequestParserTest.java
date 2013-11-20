package org.atlasapi.query.v4.schedule;

import static org.atlasapi.media.entity.Publisher.BBC;
import static org.atlasapi.media.entity.Publisher.PA;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.auth.ApplicationSourcesFetcher;
import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.annotation.ContextualAnnotationsExtractor;
import org.atlasapi.query.common.Resource;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.TimeMachine;

@RunWith(MockitoJUnitRunner.class)
public class ScheduleRequestParserTest {

    private final ApplicationSourcesFetcher applicationFetcher = mock(ApplicationSourcesFetcher.class);
    private final DateTime time = new DateTime(2012, 12, 14, 10,00,00,000, DateTimeZones.UTC);
    private final ContextualAnnotationsExtractor annotationsExtractor = mock(ContextualAnnotationsExtractor.class);
    private final ScheduleRequestParser builder = new ScheduleRequestParser(
        applicationFetcher,
        Duration.standardDays(1),
        new TimeMachine(time), annotationsExtractor 
    );

    private final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final Channel channel = Channel.builder().build();
    
    @Before
    public void setup() throws Exception {
        channel.setId(1234L);
        when(annotationsExtractor.extractFromRequest(any(HttpServletRequest.class)))
            .thenReturn(ActiveAnnotations.standard());
        when(applicationFetcher.sourcesFor(any(HttpServletRequest.class)))
            .thenReturn(Optional.of(ApplicationSources.defaults()));
    }
    
    @Test
    public void testCreatesQueryFromValidQueryString() throws Exception {
        
        Interval intvl = new Interval(new DateTime(DateTimeZones.UTC), new DateTime(DateTimeZones.UTC).plusHours(1));
        StubHttpServletRequest request = scheduleRequest(
            channel,
            intvl.getStart(), intvl.getEnd(), 
            BBC, 
            "apikey", 
            Annotation.standard(), 
            ".json"
        );
        
        ScheduleQuery query = builder.queryFrom(request);
        
        assertThat(query.getChannelId(), is(Id.valueOf(channel.getId())));
        assertThat(query.getInterval(), is(intvl));
        assertThat(query.getSource(), is(BBC));
        assertThat(query.getContext().getAnnotations().forPath(ImmutableList.of(Resource.CONTENT)), is(Annotation.standard()));
        assertThat(query.getContext().getApplicationSources(), is(ApplicationSources.defaults()));
    }
    
    @Test
    public void testCreatesQueryFromValidQueryStringWithNoExtension() throws Exception {
        
        Interval intvl = new Interval(new DateTime(DateTimeZones.UTC), new DateTime(DateTimeZones.UTC).plusHours(1));
        StubHttpServletRequest request = scheduleRequest(
            channel, 
            intvl.getStart(), intvl.getEnd(), 
            BBC, 
            "apikey", 
            Annotation.standard(), 
            ""
        );
        
        ScheduleQuery query = builder.queryFrom(request);
        
        assertThat(query.getChannelId(), is(Id.valueOf(channel.getId())));
        assertThat(query.getInterval(), is(intvl));
        assertThat(query.getSource(), is(BBC));
        assertThat(query.getContext().getAnnotations().forPath(ImmutableList.of(Resource.CONTENT)), is(Annotation.standard()));
        assertThat(query.getContext().getApplicationSources(), is(ApplicationSources.defaults()));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testDoesntAcceptQueryDurationGreaterThanMax() throws Exception {
        
        DateTime from = new DateTime(DateTimeZones.UTC);
        DateTime to = from.plusHours(25);

        StubHttpServletRequest request = scheduleRequest(channel, from, to,
            BBC, "apikey", Annotation.standard(), ".json");
        
        builder.queryFrom(request);

    }

    @Test(expected=IllegalArgumentException.class)
    public void testDoesntAcceptDisabledPublisherOutOfOpenRange() throws Exception {
        
        DateTime from = new DateTime(2012,12,06,10,00,00,000,DateTimeZones.UTC);
        DateTime to = from.plusDays(1);
        
        StubHttpServletRequest request = scheduleRequest(channel, from, to,
            PA, "apikey", Annotation.standard(), ".json");
        
        builder.queryFrom(request);
        
    }

    @Test
    public void testAcceptsDisabledPublisherAtBeginningOfRange() throws Exception {
        
        DateTime from = new DateTime(2012,12,07,00,00,00,000,DateTimeZones.UTC);
        DateTime to = from.plusHours(2);
        
        StubHttpServletRequest request = scheduleRequest(channel, from, to,
            PA, "apikey", Annotation.standard(), ".json");
        
        builder.queryFrom(request);
        
    }

    @Test
    public void testAcceptsDisabledPublisherAtEndOfRange() throws Exception {
        
        DateTime from = new DateTime(2012,12,21,00,00,00,000,DateTimeZones.UTC);
        DateTime to = from.plusHours(24);
        
        StubHttpServletRequest request = scheduleRequest(channel, from, to,
            PA, "apikey", Annotation.standard(), ".json");
        
        builder.queryFrom(request);
        
    }

    @Test(expected=IllegalArgumentException.class)
    public void testDoesntAcceptDisabledPublisherBeyondEndOfRange() throws Exception {
        
        DateTime from = new DateTime(2012,12,22,00,00,00,000,DateTimeZones.UTC);
        DateTime to = from.plusHours(24);
        
        StubHttpServletRequest request = scheduleRequest(channel, from, to,
            PA, "apikey", Annotation.standard(), ".json");
        
        builder.queryFrom(request);
    }

    private StubHttpServletRequest scheduleRequest(Channel channel, DateTime from, DateTime to, Publisher publisher, String appKey, Set<Annotation> annotations, String extension) {
        String uri = String.format(
            "http://localhost/4.0/schedules/%s%s",
            codec.encode(BigInteger.valueOf(channel.getId())),
            extension
        );
        return new StubHttpServletRequest().withRequestUri(uri)
                .withParam("from", from.toString())
                .withParam("to", to.toString())
                .withParam("source", publisher.key())
                .withParam("annotations", Joiner.on(',').join(Iterables.transform(annotations, Annotation.toKeyFunction())))
                .withParam("apiKey", appKey);
    }

}
