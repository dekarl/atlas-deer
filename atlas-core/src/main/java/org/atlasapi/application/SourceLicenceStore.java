package org.atlasapi.application;

import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;


public interface SourceLicenceStore {
    Optional<SourceLicence> licenceFor(Publisher source);
    void store(SourceLicence sourceLicence);
}
