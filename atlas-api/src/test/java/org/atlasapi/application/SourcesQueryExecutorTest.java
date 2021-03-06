package org.atlasapi.application;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;


public class SourcesQueryExecutorTest {
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final SourceIdCodec sourceIdCodec = new SourceIdCodec(idCodec);
    private SourcesQueryExecutor executor;
    
    @Before
    public void setUp() {
        executor = new SourcesQueryExecutor(sourceIdCodec);
    }
    
    @Test
    public void testAdminAccessToSource() throws QueryExecutionException {
        User user = User.builder().withId(Id.valueOf(5000)).withRole(Role.ADMIN).build();
        UserAwareQueryContext context = new UserAwareQueryContext(ApplicationSources.defaults(), 
                ActiveAnnotations.standard(),
                Optional.of(user));
        UserAwareQuery<Publisher> query = UserAwareQuery.singleQuery(Id.valueOf(1003), context);
        UserAwareQueryResult<Publisher> result = executor.execute(query);
        assertFalse(result.isListResult());
        assertEquals(result.getOnlyResource(), Publisher.YOUTUBE);
    }
    
    /**
     * Test regular user can access source they manage
     * @throws QueryExecutionException
     */
    @Test
    public void testUserOwnedSource() throws QueryExecutionException {
        User user = User.builder()
                .withId(Id.valueOf(5000))
                .withRole(Role.REGULAR)
                .withSources(ImmutableSet.of(Publisher.YOUTUBE))
                .build();
        UserAwareQueryContext context = new UserAwareQueryContext(ApplicationSources.defaults(), 
                ActiveAnnotations.standard(),
                Optional.of(user));
        UserAwareQuery<Publisher> query = UserAwareQuery.singleQuery(Id.valueOf(1003), context);
        UserAwareQueryResult<Publisher> result = executor.execute(query);
        assertFalse(result.isListResult());
        assertEquals(result.getOnlyResource(), Publisher.YOUTUBE);
    }
    
    /**
     * Test regular user can access source they do not manage
     * @throws QueryExecutionException
     */
    @Test(expected=ResourceForbiddenException.class)
    public void testNoAccessToNotManagedSource() throws QueryExecutionException {
        User user = User.builder()
                .withId(Id.valueOf(5000))
                .withRole(Role.REGULAR)
                .withSources(ImmutableSet.of(Publisher.BBC))
                .build();
        UserAwareQueryContext context = new UserAwareQueryContext(ApplicationSources.defaults(), 
                ActiveAnnotations.standard(),
                Optional.of(user));
        UserAwareQuery<Publisher> query = UserAwareQuery.singleQuery(Id.valueOf(1003), context);
        UserAwareQueryResult<Publisher> result = executor.execute(query);
        assertFalse(result.isListResult());
        assertEquals(result.getOnlyResource(), Publisher.YOUTUBE);
    }

}
