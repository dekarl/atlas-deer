package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

public class Subtitles {

    private final String languageCode;
    
    public Subtitles(String languageCode) {
        this.languageCode = checkNotNull(languageCode);
    }

    public String code() {
        return languageCode;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof Subtitles) {
            Subtitles other = (Subtitles) that;
            return languageCode.equals(other.languageCode);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return languageCode.hashCode();
    }
    
    @Override
    public String toString() {
        return languageCode;
    }
}
