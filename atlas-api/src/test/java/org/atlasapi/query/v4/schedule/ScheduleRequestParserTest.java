package org.atlasapi.query.v4.schedule;

import static org.atlasapi.media.entity.Publisher.BBC;
import static org.atlasapi.media.entity.Publisher.METABROADCAST;
import static org.atlasapi.media.entity.Publisher.PA;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.application.auth.ApplicationSourcesFetcher;
import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.annotation.ContextualAnnotationsExtractor;
import org.atlasapi.query.common.QueryParseException;
import org.atlasapi.query.common.Resource;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
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

    @Mock private ApplicationSourcesFetcher applicationFetcher;
    @Mock private ContextualAnnotationsExtractor annotationsExtractor;

    private DateTime time = new DateTime(2012, 12, 14, 10,00,00,000, DateTimeZones.UTC);
    private ScheduleRequestParser builder;

    private final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final Channel channel1 = Channel.builder().build();
    private final Channel channel2 = Channel.builder().build();
    private final ApplicationSources sources = ApplicationSources.defaults()
                .copyWithChangedReadableSourceStatus(BBC, SourceStatus.AVAILABLE_ENABLED);
    
    @Before
    public void before() throws Exception {
        builder  = new ScheduleRequestParser(
                applicationFetcher,
                Duration.standardDays(1),
                new TimeMachine(time), annotationsExtractor 
            );
        channel1.setId(1234L);
        channel2.setId(1235L);
        
        when(annotationsExtractor.extractFromRequest(any(HttpServletRequest.class)))
            .thenReturn(ActiveAnnotations.standard());
        when(applicationFetcher.sourcesFor(argThat(httpRequestWithParam("apiKey",is("apikey")))))
            .thenReturn(Optional.of(sources));
        when(applicationFetcher.sourcesFor(argThat(httpRequestWithParam("apiKey",not("apikey")))))
            .thenReturn(Optional.<ApplicationSources>absent());
    }
    
    private Matcher<HttpServletRequest> httpRequestWithParam(final String key, final Matcher<? super String> value) {
        String desc = String.format("request with param %s", key);
        return new FeatureMatcher<HttpServletRequest, String>(value, desc, "request param " + key) {

            @Override
            protected String featureValueOf(HttpServletRequest actual) {
                return actual.getParameter(key);
            }
        };
    }

    @Test
    public void testCreatesSingleQueryFromValidSingleQueryString() throws Exception {
        
        Interval intvl = new Interval(new DateTime(DateTimeZones.UTC), new DateTime(DateTimeZones.UTC).plusHours(1));
        StubHttpServletRequest request = singleScheduleRequest(
            channel1,
            intvl, 
            BBC, 
            "apikey", 
            Annotation.standard(), 
            ".json"
        );
        
        ScheduleQuery query = builder.queryFrom(request);
        
        assertThat(query.getChannelId(), is(Id.valueOf(channel1.getId())));
        assertThat(query.getInterval(), is(intvl));
        assertThat(query.getSource(), is(BBC));
        assertThat(query.getContext().getAnnotations().forPath(ImmutableList.of(Resource.CONTENT)), is(Annotation.standard()));
        assertThat(query.getContext().getApplicationSources(), is(sources));
    }
    
    @Test
    public void testCreatesSingleQueryFromValidSingleQueryStringWithNoExtension() throws Exception {
        
        Interval intvl = new Interval(new DateTime(DateTimeZones.UTC), new DateTime(DateTimeZones.UTC).plusHours(1));
        StubHttpServletRequest request = singleScheduleRequest(
            channel1, 
            intvl, 
            BBC, 
            "apikey", 
            Annotation.standard(), 
            ""
        );
        
        ScheduleQuery query = builder.queryFrom(request);
        
        assertThat(query.getChannelId(), is(Id.valueOf(channel1.getId())));
        assertThat(query.getInterval(), is(intvl));
        assertThat(query.getSource(), is(BBC));
        assertThat(query.getContext().getAnnotations().forPath(ImmutableList.of(Resource.CONTENT)), is(Annotation.standard()));
        assertThat(query.getContext().getApplicationSources(), is(sources));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testDoesntAcceptQueryDurationGreaterThanMax() throws Exception {
        
        DateTime from = new DateTime(DateTimeZones.UTC);
        DateTime to = from.plusHours(25);

        StubHttpServletRequest request = singleScheduleRequest(channel1, from, to,
            BBC, "apikey", Annotation.standard(), ".json");
        
        builder.queryFrom(request);

    }

    @Test(expected=IllegalArgumentException.class)
    public void testDoesntAcceptDisabledPublisher() throws Exception {
        
        DateTime from = new DateTime(2012,12,22,00,00,00,000,DateTimeZones.UTC);
        DateTime to = from.plusHours(24);
        
        StubHttpServletRequest request = singleScheduleRequest(channel1, from, to,
            PA, "apikey", Annotation.standard(), ".json");
        
        builder.queryFrom(request);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testDoesntAcceptUnknownApiKey() throws Exception {
        
        DateTime from = new DateTime(2012,12,22,00,00,00,000,DateTimeZones.UTC);
        DateTime to = from.plusHours(24);
        
        StubHttpServletRequest request = singleScheduleRequest(channel1, from, to,
                BBC, "unknownapikey", Annotation.standard(), ".json");

        builder.queryFrom(request);
        
    }

    @Test(expected=IllegalArgumentException.class)
    public void testDoesntAcceptRequestWithoutSource() throws Exception {
        
        DateTime from = new DateTime(2012,12,22,00,00,00,000,DateTimeZones.UTC);
        DateTime to = from.plusHours(24);
        
        StubHttpServletRequest request = singleScheduleRequest(channel1, from, to,
                null, "unknownapikey", Annotation.standard(), ".json");
        
        builder.queryFrom(request);
        
    }

    @Test(expected=QueryParseException.class)
    public void testDoesntAccpetRequestWithInvalidId() throws Exception {
        
        DateTime from = new DateTime(2012,12,22,00,00,00,000,DateTimeZones.UTC);
        DateTime to = from.plusHours(24);
        
        StubHttpServletRequest request = singleScheduleRequest("invalid", from, to,
                BBC, "apikey", Annotation.standard(), ".json");
        
        builder.queryFrom(request);
        
    }
    
    @Test
    public void testCreatesMultiScheduleFromValidMultiScheduleQueryString() throws Exception {
        
        Interval intvl = new Interval(new DateTime(DateTimeZones.UTC), new DateTime(DateTimeZones.UTC).plusHours(1));
        StubHttpServletRequest request = multiScheduleRequest(
            ImmutableList.of(channel1, channel2), 
            intvl, 
            BBC, 
            "apikey", 
            Annotation.standard(), 
            ""
        );
        ScheduleQuery query = builder.queryFrom(request);
        assertTrue(query.isMultiChannel());
        assertThat(query.getChannelIds().size(), is(2));
        assertThat(query.getChannelIds().asList().get(0), is(Id.valueOf(channel1.getId())));
        assertThat(query.getChannelIds().asList().get(1), is(Id.valueOf(channel2.getId())));
    }

    @Test(expected=QueryParseException.class)
    public void testDoesntAcceptMultiScheduleFromQueryWithInvalidIds() throws Exception {
        
        Interval intvl = new Interval(new DateTime(DateTimeZones.UTC), new DateTime(DateTimeZones.UTC).plusHours(1));
        StubHttpServletRequest request = multiScheduleRequest(
            "hkqs,invalid", 
            intvl, 
            BBC, 
            "apikey", 
            Annotation.standard(), 
            ""
        );
        
        ScheduleQuery query = builder.queryFrom(request);
        assertTrue(query.isMultiChannel());
        assertThat(query.getChannelIds().size(), is(2));
        assertThat(query.getChannelIds().asList().get(0), is(Id.valueOf(channel1.getId())));
        assertThat(query.getChannelIds().asList().get(1), is(Id.valueOf(channel2.getId())));
    }
    
    @Test
    public void testCreatesSingleQueryFromQueryWithNoApiKeyForDefaultEnabledSource() throws Exception {
        
        Interval intvl = new Interval(new DateTime(DateTimeZones.UTC), new DateTime(DateTimeZones.UTC).plusHours(1));
        StubHttpServletRequest request = singleScheduleRequest(
            channel1, 
            intvl, 
            METABROADCAST,
            null, 
            Annotation.standard(), 
            ""
        );
        
        ScheduleQuery query = builder.queryFrom(request);
        
        assertThat(query.getChannelId(), is(Id.valueOf(channel1.getId())));
        assertThat(query.getInterval(), is(intvl));
        assertThat(query.getSource(), is(METABROADCAST));
        assertThat(query.getContext().getAnnotations().forPath(ImmutableList.of(Resource.CONTENT)), is(Annotation.standard()));
        assertThat(query.getContext().getApplicationSources(), is(sources));
    }

    private StubHttpServletRequest multiScheduleRequest(List<Channel> channels, Interval intvl,
            Publisher src, String appKey, Set<Annotation> annotations, String ext) {
        String ids = Joiner.on(',').join(Iterables.transform(channels,
            new Function<Channel, String>() {
                @Override
                public String apply(Channel channel) {
                    return codec.encode(BigInteger.valueOf(channel.getId()));
                }
            }
        ));
        return multiScheduleRequest(ids, intvl, src, appKey, annotations, ext);
    }

    private StubHttpServletRequest multiScheduleRequest(String ids, Interval intvl, Publisher src,
            String appKey, Set<Annotation> annotations, String ext) {
        String resource = String.format("http://localhost/4/schedules%s", ext);
        StubHttpServletRequest req = createScheduleRequest(resource, intvl.getStart(), intvl.getEnd(), src, appKey, annotations);
        return req.withParam("id", ids);
    }

    private StubHttpServletRequest singleScheduleRequest(Channel channel, Interval interval, Publisher publisher, String appKey, Set<Annotation> annotations, String extension) {
        return singleScheduleRequest(channel, interval.getStart(), interval.getEnd(), publisher, appKey, annotations, extension);
    }
    private StubHttpServletRequest singleScheduleRequest(Channel channel, DateTime from, DateTime to, Publisher publisher, String appKey, Set<Annotation> annotations, String extension) {
        String channelKey = codec.encode(BigInteger.valueOf(channel.getId()));
        return singleScheduleRequest(channelKey,
                from,
                to,
                publisher,
                appKey,
                annotations,
                extension);
    }

    private StubHttpServletRequest singleScheduleRequest(String channelKey, DateTime from,
            DateTime to, Publisher publisher, String appKey, Set<Annotation> annotations,
            String extension) {
        String resource = String.format("http://localhost/4/schedules/%s%s",
            channelKey, extension
        );
        return createScheduleRequest(resource, from, to, publisher, appKey, annotations);
    }

    private StubHttpServletRequest createScheduleRequest(String resource, DateTime from, DateTime to,
            Publisher publisher, String appKey, Set<Annotation> annotations) {
        return new StubHttpServletRequest().withRequestUri(resource)
                .withParam("from", from.toString())
                .withParam("to", to.toString())
                .withParam("source", publisher == null ? null : publisher.key())
                .withParam("annotations", Joiner.on(',').join(Iterables.transform(annotations, Annotation.toKeyFunction())))
                .withParam("apiKey", appKey);
    }

}
