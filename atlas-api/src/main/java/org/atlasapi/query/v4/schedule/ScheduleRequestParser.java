package org.atlasapi.query.v4.schedule;

import static com.google.common.base.Preconditions.checkArgument;
import static com.metabroadcast.common.webapp.query.DateTimeInQueryParser.queryDateTimeParser;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.application.auth.ApplicationSourcesFetcher;
import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.annotation.ContextualAnnotationsExtractor;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryParseException;
import org.atlasapi.query.common.SetBasedRequestParameterValidator;
import org.atlasapi.source.Sources;
import org.elasticsearch.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.webapp.query.DateTimeInQueryParser;

class ScheduleRequestParser {
    
    private static final Pattern CHANNEL_ID_PATTERN = Pattern.compile(
        ".*schedules/([^.]+)(.[\\w\\d.]+)?$"
    );
    
    private final ApplicationSourcesFetcher applicationStore;

    private final SetBasedRequestParameterValidator validator = SetBasedRequestParameterValidator.builder()
        .withRequiredParameters("from","to","source")
        .withOptionalParameters("annotations","apiKey", "callback")
        .build();
    
    private final NumberToShortStringCodec idCodec;
    private final DateTimeInQueryParser dateTimeParser;
    private final ContextualAnnotationsExtractor annotationExtractor;
    private final Duration maxQueryDuration;
    private final Clock clock;

    public ScheduleRequestParser(ApplicationSourcesFetcher appFetcher, Duration maxQueryDuration, Clock clock, ContextualAnnotationsExtractor annotationsExtractor) {
        this.applicationStore = appFetcher;
        this.maxQueryDuration = maxQueryDuration;
        this.idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.dateTimeParser = queryDateTimeParser()
                .parsesIsoDateTimes()
                .parsesIsoTimes()
                .parsesIsoDates()
                .parsesOffsets()
                .build();
        this.clock = clock;
        this.annotationExtractor = annotationsExtractor;
    }

    public ScheduleQuery queryFrom(HttpServletRequest request) throws QueryParseException, NotFoundException, InvalidApiKeyException {
        Id channel = extractChannel(request);

        validator.validateParameters(request);
        
        Publisher publisher = extractPublisher(request);
        Interval queryInterval = extractInterval(request);
        
        ApplicationSources appSources = getConfiguration(request);
        appSources = appConfigForValidPublisher(publisher, appSources, queryInterval);
        checkArgument(appSources != null, "Source %s not enabled", publisher);
        
        ActiveAnnotations annotations = annotationExtractor.extractFromRequest(request);

        return new ScheduleQuery(publisher, channel, queryInterval, new QueryContext(appSources, annotations));
    }

    private ApplicationSources appConfigForValidPublisher(Publisher publisher,
                                                                ApplicationSources appSources,
                                                                Interval interval) {
        if (appSources.isReadEnabled(publisher)) {
            return appSources;
        }
        if (Publisher.PA.equals(publisher) && overlapsOpenInterval(interval)) {
            List<SourceReadEntry> reads = ImmutableList.<SourceReadEntry>builder().addAll(appSources.getReads())
                    .add(new SourceReadEntry(Publisher.PA, SourceStatus.AVAILABLE_ENABLED))
                    .build();
            appSources = appSources.copy().withReadableSources(reads).build();
            return appSources;
        }
        return null;
    }

    private boolean overlapsOpenInterval(Interval interval) {
        DateTime now = clock.now().toLocalDate().toDateTimeAtStartOfDay();
        Interval openInterval = new Interval(now.minusDays(7), now.plusDays(8));
        return openInterval.contains(interval);
    }

    private Id extractChannel(HttpServletRequest request) throws QueryParseException, NotFoundException {
        String channelId = getChannelId(request.getRequestURI());
        
        Id cid;
        try {
            cid = Id.valueOf(idCodec.decode(channelId));
        } catch (IllegalArgumentException e) {
            throw new QueryParseException("Invalid id " + channelId);
        }
        return cid;
    }

    private String getChannelId(String requestUri) {
        Matcher matcher = CHANNEL_ID_PATTERN.matcher(requestUri);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException("Channel identifier missing");
    }
    
    private Interval extractInterval(HttpServletRequest request) {
        DateTime from = dateTimeParser.parse(getParameter(request, "from"));
        DateTime to = dateTimeParser.parse(getParameter(request, "to"));
        
        Interval queryInterval = new Interval(from, to);
        checkArgument(!queryInterval.toDuration().isLongerThan(maxQueryDuration), "Query interval cannot be longer than %s", maxQueryDuration);
        return queryInterval;
    }

    private Publisher extractPublisher(HttpServletRequest request) {
        String pubKey = getParameter(request, "source");
        Optional<Publisher> publisher = Sources.fromPossibleKey(pubKey);
        checkArgument(publisher.isPresent(), "Unknown source %s", pubKey);
        return publisher.get();
    }

    private ApplicationSources getConfiguration(HttpServletRequest request) throws InvalidApiKeyException {
        Optional<ApplicationSources> config = applicationStore.sourcesFor(request);
        if (config.isPresent()) {
            return config.get();
        }
        String apiKeyParam = request.getParameter("apiKey");
        // request doesn't specify apiKey so use default configuration.
        if (apiKeyParam == null) {
            return ApplicationSources.defaults();
        }
        // the request has an apiKey param but no config is found.
        throw new IllegalArgumentException("Unknown API key " + apiKeyParam);
    }

    private String getParameter(HttpServletRequest request, String param) {
        String paramValue = request.getParameter(param);
        checkArgument(!Strings.isNullOrEmpty(paramValue), "Missing required parameter %s", param);
        return paramValue;
    }

}
