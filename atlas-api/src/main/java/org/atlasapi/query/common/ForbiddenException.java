package org.atlasapi.query.common;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.entity.Id;

public class ForbiddenException extends QueryExecutionException {
    
    private final Id forbiddenResource;
    
    public ForbiddenException(Id forbiddenResouce) {
        this.forbiddenResource = checkNotNull(forbiddenResouce);
    }

    public Id getForbiddenResource() {
        return forbiddenResource;
    }
    
    @Override
    public String getMessage() {
        return forbiddenResource.toString();
    }
    
}
