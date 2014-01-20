package org.atlasapi.query.common;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.content.Content;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.topic.Topic;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.servlet.StubHttpServletRequest;

@RunWith(MockitoJUnitRunner.class)
public class ContextualQueryParserTest {

    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final QueryAttributeParser attributeParser = mock(QueryAttributeParser.class);
    private final ContextualQueryContextParser queryContextParser = mock(ContextualQueryContextParser.class);
    private ContextualQueryParser<Topic, Content> parser;
    
    @BeforeClass
    public void setUp() {
        when(attributeParser.getParameterNames()).thenReturn(ImmutableSet.of("alias.namespace"));
        when(queryContextParser.getParameterNames()).thenReturn(ImmutableSet.<String>of());
        this.parser = new ContextualQueryParser<Topic, Content>(Resource.TOPIC, Attributes.TOPIC_ID,
                Resource.CONTENT, idCodec, attributeParser, queryContextParser);
    }
    
    @Test
    public void testParseRequest() throws Exception {
        
        HttpServletRequest req = new StubHttpServletRequest().withRequestUri(
            "/4.0/topics/cbbh/content.json"
        ).withParam("alias.namespace", "ns");
        
        when(attributeParser.parse(req))
            .thenReturn(new AttributeQuerySet(ImmutableSet.<AttributeQuery<?>>of()));
        when(queryContextParser.parseContext(req))
            .thenReturn(new QueryContext(ApplicationSources.defaults(), ActiveAnnotations.standard()));
        
        ContextualQuery<Topic,Content> query = parser.parse(req);
        
        Id contextId = query.getContextQuery().getOnlyId();
        assertThat(idCodec.encode(contextId.toBigInteger()), is("cbbh"));
        
        AttributeQuerySet resourceQuerySet = query.getResourceQuery().getOperands();
        AttributeQuery<?> contextAttributeQuery = Iterables.getOnlyElement(resourceQuerySet);
        
        assertThat((Id)contextAttributeQuery.getValue().get(0), is(Id.valueOf(idCodec.decode("cbbh"))));
        
        assertTrue(query.getContext() == query.getContextQuery().getContext());
        assertTrue(query.getContext() == query.getResourceQuery().getContext());
        
        verify(attributeParser, times(1)).parse(req);
        verify(queryContextParser, times(1)).parseContext(req);
        
    }

}
