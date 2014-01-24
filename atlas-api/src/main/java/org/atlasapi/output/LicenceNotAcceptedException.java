package org.atlasapi.output;

import org.atlasapi.query.common.QueryExecutionException;


public class LicenceNotAcceptedException extends QueryExecutionException {
    private static final long serialVersionUID = 7886280626313023470L;

    public LicenceNotAcceptedException() {
        super("Licence terms must be accepted");
    }
}
