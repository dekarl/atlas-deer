package org.atlasapi.application;

import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;

import com.google.common.base.Optional;


public class SourceLicenceQueryExecutor implements UserAwareQueryExecutor<SourceLicence> {
    
    private final SourceIdCodec sourceIdCodec;
    private final SourceLicenceStore store;

    public SourceLicenceQueryExecutor(SourceIdCodec sourceIdCodec, SourceLicenceStore store) {
        super();
        this.sourceIdCodec = sourceIdCodec;
        this.store = store;
    }

    @Override
    public UserAwareQueryResult<SourceLicence> execute(UserAwareQuery<SourceLicence> query)
            throws QueryExecutionException {
        return query.isListQuery() ? multipleQuery(query) : singleQuery(query);
    }

    private UserAwareQueryResult<SourceLicence> singleQuery(UserAwareQuery<SourceLicence> query) throws NotFoundException{
        Optional<Publisher> source = sourceIdCodec.decode(query.getOnlyId());
        // TODO CHECK PERMISSION
        if (source.isPresent()) {
            Optional<SourceLicence> licence = store.licenceFor(source.get());
            if (licence.isPresent()) {
                return UserAwareQueryResult.singleResult(licence.get(), query.getContext());
            }
        } 
        // If we get to here then there was no licence for the given id
        throw new NotFoundException(query.getOnlyId());        
    }
    

    
    private UserAwareQueryResult<SourceLicence> multipleQuery(UserAwareQuery<SourceLicence> query) {
        throw new UnsupportedOperationException();
    }

}
