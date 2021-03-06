/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.query;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.content.Content;
import org.atlasapi.equivalence.DefaultMergingEquivalentsResolver;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.output.OutputContentMerger;
import org.atlasapi.output.StrategyBackedEquivalentsMerger;
import org.atlasapi.query.common.ContextualQueryExecutor;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.v4.content.IndexBackedEquivalentContentQueryExecutor;
import org.atlasapi.query.v4.schedule.EquivalentScheduleResolverBackedScheduleQueryExecutor;
import org.atlasapi.query.v4.schedule.ScheduleQueryExecutor;
import org.atlasapi.query.v4.search.support.ContentResolvingSearcher;
import org.atlasapi.query.v4.topic.IndexBackedTopicQueryExecutor;
import org.atlasapi.query.v4.topic.TopicContentQueryExecutor;
import org.atlasapi.search.SearchResolver;
import org.atlasapi.topic.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
//@Import(EquivModule.class)
public class QueryModule {

    private @Autowired AtlasPersistenceModule persistenceModule;

    @Bean QueryExecutor<Topic> topicQueryExecutor() {
        return new IndexBackedTopicQueryExecutor(persistenceModule.topicIndex(), persistenceModule.topicStore());
    }
    
    @Bean
    public ContextualQueryExecutor<Topic, Content> topicContentQueryExecutor() {
        return new TopicContentQueryExecutor(persistenceModule.topicStore(), persistenceModule.contentIndex(), mergingContentResolver());
    }
    
    @Bean
    public QueryExecutor<Content> contentQueryExecutor() {
        return new IndexBackedEquivalentContentQueryExecutor(persistenceModule.contentIndex(), 
            mergingContentResolver());
    }

    private MergingEquivalentsResolver<Content> mergingContentResolver() {
        return new DefaultMergingEquivalentsResolver<Content>(
            persistenceModule.getEquivalentContentStore(), 
            equivalentsMerger()
        );
    }

    private StrategyBackedEquivalentsMerger<Content> equivalentsMerger() {
        return new StrategyBackedEquivalentsMerger<Content>(new OutputContentMerger());
    }
    
    @Qualifier("store")
    @Bean ScheduleQueryExecutor equivalentScheduleStoreScheduleQueryExecutor() {
        return new EquivalentScheduleResolverBackedScheduleQueryExecutor(persistenceModule.channelStore(), 
            persistenceModule.getEquivalentScheduleStore(), equivalentsMerger());
    }

    @Bean
    public SearchResolver v4SearchResolver() {
        // FIXME externalize timeout
        return new ContentResolvingSearcher(persistenceModule.contentSearcher(), persistenceModule.contentStore(), 60000);
    }

}
