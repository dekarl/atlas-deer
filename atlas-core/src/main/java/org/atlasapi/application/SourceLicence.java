package org.atlasapi.application;

import org.atlasapi.media.entity.Publisher;


public class SourceLicence {
    
    private final Publisher source;
    private final String licence;
    
    private SourceLicence(Publisher source, String licence) {
        super();
        this.source = source;
        this.licence = licence;
    }

    public Publisher getSource() {
        return source;
    }

    public String getLicence() {
        return licence;
    }
    
    public static SourceLicenceBuilder builder() {
        return new SourceLicenceBuilder();
    }
    
    public static class SourceLicenceBuilder {
        private Publisher source;
        private String licence;
        
        public SourceLicenceBuilder withSource(Publisher source) {
            this.source = source;
            return this;
        }
        
        public SourceLicenceBuilder withLicence(String licence) {
            this.licence = licence;
            return this;
        }
        
        public SourceLicence build() {
            return new SourceLicence(source, licence);
        }
    }
    
    

}
