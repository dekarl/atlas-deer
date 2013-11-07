package org.atlasapi.query;

import static org.atlasapi.output.Annotation.AVAILABLE_LOCATIONS;
import static org.atlasapi.output.Annotation.BRAND_REFERENCE;
import static org.atlasapi.output.Annotation.BRAND_SUMMARY;
import static org.atlasapi.output.Annotation.BROADCASTS;
import static org.atlasapi.output.Annotation.CHANNEL;
import static org.atlasapi.output.Annotation.CHANNELS;
import static org.atlasapi.output.Annotation.CHANNEL_SUMMARY;
import static org.atlasapi.output.Annotation.CLIPS;
import static org.atlasapi.output.Annotation.CONTENT_DETAIL;
import static org.atlasapi.output.Annotation.CONTENT_SUMMARY;
import static org.atlasapi.output.Annotation.DESCRIPTION;
import static org.atlasapi.output.Annotation.EXTENDED_DESCRIPTION;
import static org.atlasapi.output.Annotation.EXTENDED_ID;
import static org.atlasapi.output.Annotation.FIRST_BROADCASTS;
import static org.atlasapi.output.Annotation.ID;
import static org.atlasapi.output.Annotation.ID_SUMMARY;
import static org.atlasapi.output.Annotation.KEY_PHRASES;
import static org.atlasapi.output.Annotation.LOCATIONS;
import static org.atlasapi.output.Annotation.NEXT_BROADCASTS;
import static org.atlasapi.output.Annotation.PEOPLE;
import static org.atlasapi.output.Annotation.RECENTLY_BROADCAST;
import static org.atlasapi.output.Annotation.RELATED_LINKS;
import static org.atlasapi.output.Annotation.SERIES_REFERENCE;
import static org.atlasapi.output.Annotation.SERIES_SUMMARY;
import static org.atlasapi.output.Annotation.SUB_ITEMS;
import static org.atlasapi.output.Annotation.TOPICS;
import static org.atlasapi.output.Annotation.UPCOMING;

import org.atlasapi.application.auth.ApplicationSourcesFetcher;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.content.criteria.attribute.Attributes;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.content.ContentType;
import org.atlasapi.media.content.schedule.ScheduleIndex;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.topic.PopularTopicIndex;
import org.atlasapi.media.topic.Topic;
import org.atlasapi.media.topic.TopicResolver;
import org.atlasapi.output.Annotation;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.annotation.AvailableLocationsAnnotation;
import org.atlasapi.output.annotation.BrandReferenceAnnotation;
import org.atlasapi.output.annotation.BrandSummaryAnnotation;
import org.atlasapi.output.annotation.BroadcastsAnnotation;
import org.atlasapi.output.annotation.ChannelAnnotation;
import org.atlasapi.output.annotation.ChannelSummaryWriter;
import org.atlasapi.output.annotation.ChannelsAnnotation;
import org.atlasapi.output.annotation.ClipsAnnotation;
import org.atlasapi.output.annotation.ContentDescriptionAnnotation;
import org.atlasapi.output.annotation.DescriptionAnnotation;
import org.atlasapi.output.annotation.ExtendedDescriptionAnnotation;
import org.atlasapi.output.annotation.ExtendedIdentificationAnnotation;
import org.atlasapi.output.annotation.FirstBroadcastAnnotation;
import org.atlasapi.output.annotation.IdentificationAnnotation;
import org.atlasapi.output.annotation.IdentificationSummaryAnnotation;
import org.atlasapi.output.annotation.KeyPhrasesAnnotation;
import org.atlasapi.output.annotation.LocationsAnnotation;
import org.atlasapi.output.annotation.NextBroadcastAnnotation;
import org.atlasapi.output.annotation.NullWriter;
import org.atlasapi.output.annotation.PeopleAnnotation;
import org.atlasapi.output.annotation.RecentlyBroadcastAnnotation;
import org.atlasapi.output.annotation.RelatedLinksAnnotation;
import org.atlasapi.output.annotation.SeriesReferenceAnnotation;
import org.atlasapi.output.annotation.SeriesSummaryAnnotation;
import org.atlasapi.output.annotation.SubItemAnnotation;
import org.atlasapi.output.annotation.TopicsAnnotation;
import org.atlasapi.output.annotation.UpcomingAnnotation;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.output.MongoContainerSummaryResolver;
import org.atlasapi.persistence.output.MongoRecentlyBroadcastChildrenResolver;
import org.atlasapi.persistence.output.MongoUpcomingChildrenResolver;
import org.atlasapi.persistence.output.RecentlyBroadcastChildrenResolver;
import org.atlasapi.persistence.output.UpcomingChildrenResolver;
import org.atlasapi.query.annotation.ResourceAnnotationIndex;
import org.atlasapi.query.common.AttributeCoercers;
import org.atlasapi.query.common.ContextualQueryContextParser;
import org.atlasapi.query.common.ContextualQueryParser;
import org.atlasapi.query.common.IndexAnnotationsExtractor;
import org.atlasapi.query.common.IndexContextualAnnotationsExtractor;
import org.atlasapi.query.common.QueryAtomParser;
import org.atlasapi.query.common.QueryAttributeParser;
import org.atlasapi.query.common.QueryContextParser;
import org.atlasapi.query.common.Resource;
import org.atlasapi.query.common.StandardQueryParser;
import org.atlasapi.query.v4.content.ContentController;
import org.atlasapi.query.v4.schedule.ChannelListWriter;
import org.atlasapi.query.v4.schedule.ContentListWriter;
import org.atlasapi.query.v4.schedule.ScheduleController;
import org.atlasapi.query.v4.schedule.ScheduleIndexDebugController;
import org.atlasapi.query.v4.schedule.ScheduleQueryResultWriter;
import org.atlasapi.query.v4.search.ContentQueryResultWriter;
import org.atlasapi.query.v4.search.SearchController;
import org.atlasapi.query.v4.topic.PopularTopicController;
import org.atlasapi.query.v4.topic.TopicContentController;
import org.atlasapi.query.v4.topic.TopicContentResultWriter;
import org.atlasapi.query.v4.topic.TopicController;
import org.atlasapi.query.v4.topic.TopicListWriter;
import org.atlasapi.query.v4.topic.TopicQueryResultWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;
import com.metabroadcast.common.time.SystemClock;

@Configuration
@Import({QueryModule.class})
public class QueryWebModule {
    
    private @Value("${local.host.name}") String localHostName;
    private @Value("${ids.expose}") String exposeIds;
    
    private @Autowired DatabasedMongo mongo;
    private @Autowired QueryModule queryModule;
//    private @Autowired ContentGroupResolver contentGroupResolver;
    private @Autowired ChannelResolver channelResolver;
    private @Autowired SearchResolver v4SearchResolver;
    private @Autowired TopicResolver topicResolver;
    private @Autowired PopularTopicIndex popularTopicIndex;
//    private @Autowired SegmentResolver segmentResolver;
//    private @Autowired ProductResolver productResolver;
    private @Autowired ScheduleIndex scheduleIndex;
    private @Autowired UserFetcher userFetcher;

    private @Autowired ApplicationSourcesFetcher configFetcher;

    @Bean NumberToShortStringCodec idCodec() {
        return SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    @Bean SelectionBuilder  selectionBuilder() {
        return Selection.builder().withDefaultLimit(50).withMaxLimit(100);
    }

    @Bean
    ScheduleController v4ScheduleController() {
        return new ScheduleController(queryModule.scheduleStoreScheduleQueryExecutor(),
            configFetcher, new ScheduleQueryResultWriter(channelListWriter(), contentListWriter()),
            new IndexContextualAnnotationsExtractor(ResourceAnnotationIndex.combination()
                .addExplicitSingleContext(channelAnnotationIndex())
                .addExplicitListContext(contentAnnotationIndex())
                .combine()));
    }

    @Bean
    TopicController v4TopicController() {
        return new TopicController(topicQueryParser(), 
            queryModule.topicQueryExecutor(), new TopicQueryResultWriter(topicListWriter()));
    }
    
    @Bean
    ContentController contentController() {
        return new ContentController(contentQueryParser(),
            queryModule.contentQueryExecutor(), new ContentQueryResultWriter(contentListWriter()));
    }

    @Bean
    TopicContentController topicContentController() {
        ContextualQueryContextParser contextParser = new ContextualQueryContextParser(configFetcher,
                userFetcher,
            new IndexContextualAnnotationsExtractor(ResourceAnnotationIndex.combination()
                .addImplicitListContext(contentAnnotationIndex())
                .addExplicitSingleContext(topicAnnotationIndex())
                .combine()
            ), selectionBuilder());
        
        ContextualQueryParser<Topic, Content> parser = new ContextualQueryParser<Topic, Content>(
            Resource.TOPIC, Attributes.TOPIC_ID, Resource.CONTENT, idCodec(),
            contentQueryAttributeParser(),
            contextParser);
        
        return new TopicContentController(parser, queryModule.topicContentQueryExecutor(),
                new TopicContentResultWriter(topicListWriter(), contentListWriter()));
    }

    private QueryAttributeParser contentQueryAttributeParser() {
        return new QueryAttributeParser(ImmutableList.of(
            QueryAtomParser.valueOf(Attributes.ID, AttributeCoercers.idCoercer(idCodec())),
            QueryAtomParser.valueOf(Attributes.CONTENT_TYPE, AttributeCoercers.enumCoercer(ContentType.fromKey())),
            QueryAtomParser.valueOf(Attributes.SOURCE, AttributeCoercers.enumCoercer(Publisher.fromKey())),
            QueryAtomParser.valueOf(Attributes.ALIASES_NAMESPACE, AttributeCoercers.stringCoercer()),
            QueryAtomParser.valueOf(Attributes.ALIASES_VALUE, AttributeCoercers.stringCoercer()),
            QueryAtomParser.valueOf(Attributes.TOPIC_ID, AttributeCoercers.idCoercer(idCodec())),
            QueryAtomParser.valueOf(Attributes.TOPIC_RELATIONSHIP, AttributeCoercers.stringCoercer()),
            QueryAtomParser.valueOf(Attributes.TOPIC_SUPERVISED, AttributeCoercers.booleanCoercer()),
            QueryAtomParser.valueOf(Attributes.TOPIC_WEIGHTING, AttributeCoercers.floatCoercer())
        ));
    }
    
    private StandardQueryParser<Content> contentQueryParser() {
        QueryContextParser contextParser = new QueryContextParser(configFetcher,
                userFetcher,
        new IndexAnnotationsExtractor(contentAnnotationIndex()), selectionBuilder());
        
        return new StandardQueryParser<Content>(Resource.CONTENT, 
                contentQueryAttributeParser(),
                idCodec(), contextParser);
    }

    private StandardQueryParser<Topic> topicQueryParser() {
        QueryContextParser contextParser = new QueryContextParser(configFetcher, userFetcher,
        new IndexAnnotationsExtractor(topicAnnotationIndex()), selectionBuilder());
        
        return new StandardQueryParser<Topic>(Resource.TOPIC, 
            new QueryAttributeParser(ImmutableList.of(
                QueryAtomParser.valueOf(Attributes.ID, AttributeCoercers.idCoercer(idCodec())),
                QueryAtomParser.valueOf(Attributes.TOPIC_TYPE, AttributeCoercers.enumCoercer(Topic.Type.fromKey())),
                QueryAtomParser.valueOf(Attributes.SOURCE, AttributeCoercers.enumCoercer(Publisher.fromKey())),
                QueryAtomParser.valueOf(Attributes.ALIASES_NAMESPACE, AttributeCoercers.stringCoercer()),
                QueryAtomParser.valueOf(Attributes.ALIASES_VALUE, AttributeCoercers.stringCoercer())
            )),
            idCodec(), contextParser
        );
    }

    @Bean
    PopularTopicController popularTopicController() {
        return new PopularTopicController(topicResolver, popularTopicIndex, new TopicQueryResultWriter(topicListWriter()), configFetcher);
    }

    @Bean
    SearchController searchController() {
        return new SearchController(v4SearchResolver, configFetcher, new ContentQueryResultWriter(contentListWriter()));
    }
    
    @Bean
    ResourceAnnotationIndex contentAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.CONTENT, Annotation.all())
            .attach(Annotation.TOPICS, topicAnnotationIndex(), Annotation.ID)
            .build();
    }
    
    @Bean
    ResourceAnnotationIndex topicAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.TOPIC, Annotation.all()).build();
    }

    @Bean
    ResourceAnnotationIndex channelAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.CHANNEL, Annotation.all()).build();
    }
    
    @Bean
    EntityListWriter<Content> contentListWriter() {
        ImmutableSet<Annotation> commonImplied = ImmutableSet.of(ID_SUMMARY);
        RecentlyBroadcastChildrenResolver recentlyBroadcastResolver = new MongoRecentlyBroadcastChildrenResolver(mongo);
        UpcomingChildrenResolver upcomingChildrenResolver = new MongoUpcomingChildrenResolver(mongo);
        MongoContainerSummaryResolver containerSummaryResolver = new MongoContainerSummaryResolver(mongo, idCodec());
        return new ContentListWriter(AnnotationRegistry.<Content>builder()
            .registerDefault(ID_SUMMARY, new IdentificationSummaryAnnotation(idCodec()))
            .register(ID, new IdentificationAnnotation(), commonImplied)
            .register(EXTENDED_ID, new ExtendedIdentificationAnnotation(idCodec()), ImmutableSet.of(ID))
            .register(SERIES_REFERENCE, new SeriesReferenceAnnotation(idCodec()), commonImplied)
            .register(SERIES_SUMMARY, new SeriesSummaryAnnotation(idCodec(), containerSummaryResolver), commonImplied, ImmutableSet.of(SERIES_REFERENCE))
            .register(BRAND_REFERENCE, new BrandReferenceAnnotation(idCodec()), commonImplied)
            .register(BRAND_SUMMARY, new BrandSummaryAnnotation(idCodec(), containerSummaryResolver), commonImplied, ImmutableSet.of(BRAND_REFERENCE))
            .register(DESCRIPTION, new ContentDescriptionAnnotation(), ImmutableSet.of(ID, SERIES_REFERENCE, BRAND_REFERENCE))
            .register(EXTENDED_DESCRIPTION, new ExtendedDescriptionAnnotation(), ImmutableSet.of(DESCRIPTION, EXTENDED_ID))
            .register(SUB_ITEMS, new SubItemAnnotation(idCodec()), commonImplied)
            .register(CLIPS, new ClipsAnnotation(), commonImplied)
            .register(PEOPLE, new PeopleAnnotation(), commonImplied)
            .register(TOPICS, new TopicsAnnotation(topicResolver, topicListWriter()), commonImplied)
            //.register(CONTENT_GROUPS, new ContentGroupsAnnotation(contentGroupResolver), commonImplied)
            //.register(SEGMENT_EVENTS, new SegmentEventsAnnotation(segmentResolver), commonImplied)
            .register(RELATED_LINKS, new RelatedLinksAnnotation(), commonImplied)
            .register(KEY_PHRASES, new KeyPhrasesAnnotation(), commonImplied)
            .register(LOCATIONS, new LocationsAnnotation(), commonImplied)
            .register(BROADCASTS, new BroadcastsAnnotation(), commonImplied)
            .register(FIRST_BROADCASTS, new FirstBroadcastAnnotation(), commonImplied)
            .register(NEXT_BROADCASTS, new NextBroadcastAnnotation(new SystemClock()), commonImplied)
            .register(AVAILABLE_LOCATIONS, new AvailableLocationsAnnotation(), commonImplied)
            .register(UPCOMING, new UpcomingAnnotation(idCodec(), upcomingChildrenResolver), commonImplied)
            //.register(PRODUCTS, new ProductsAnnotation(productResolver), commonImplied)
            .register(RECENTLY_BROADCAST, new RecentlyBroadcastAnnotation(idCodec(), recentlyBroadcastResolver), commonImplied)
            .register(CHANNELS, new ChannelsAnnotation(), commonImplied)
            .register(CONTENT_SUMMARY, NullWriter.create(Content.class), ImmutableSet.of(DESCRIPTION, BRAND_SUMMARY, 
                SERIES_SUMMARY, BROADCASTS, LOCATIONS))
            .register(CONTENT_DETAIL, NullWriter.create(Content.class), ImmutableSet.of(EXTENDED_DESCRIPTION, SUB_ITEMS, CLIPS, 
                PEOPLE, BRAND_SUMMARY, SERIES_SUMMARY, BROADCASTS, LOCATIONS, KEY_PHRASES, RELATED_LINKS))
        .build());
    }

    @Bean
    protected EntityListWriter<Topic> topicListWriter() {
        return new TopicListWriter(AnnotationRegistry.<Topic>builder()
            .registerDefault(ID_SUMMARY, new IdentificationSummaryAnnotation(idCodec()))
            .register(ID, new IdentificationAnnotation(), ID_SUMMARY)
            .register(EXTENDED_ID, new ExtendedIdentificationAnnotation(idCodec()), ImmutableSet.of(ID))
            .register(DESCRIPTION, new DescriptionAnnotation<Topic>(), ImmutableSet.of(ID))
            .build());
    }
    
    @Bean
    protected EntityListWriter<Channel> channelListWriter() {
        return new ChannelListWriter(AnnotationRegistry.<Channel>builder()
            .registerDefault(ID_SUMMARY, new IdentificationSummaryAnnotation(idCodec()))
            .register(ID, new IdentificationAnnotation(), ID_SUMMARY)
            .register(EXTENDED_ID, new ExtendedIdentificationAnnotation(idCodec()), ImmutableSet.of(ID))
            .register(CHANNEL_SUMMARY, new ChannelSummaryWriter(), ImmutableSet.of(ID))
            .register(CHANNEL, new ChannelAnnotation(), ImmutableSet.of(CHANNEL_SUMMARY))
            .build());
    }
    
    @Bean
    public ScheduleIndexDebugController scheduleIndexDebug() {
        return new ScheduleIndexDebugController(scheduleIndex, channelResolver, configFetcher);
    }
}
