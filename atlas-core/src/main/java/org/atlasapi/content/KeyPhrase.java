package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;

public final class KeyPhrase {

    private String phrase;
    private Double weighting;
    
    public KeyPhrase(String phrase, Double weighting) {
        this.phrase = checkNotNull(phrase);
        this.weighting = weighting;
    }
    
    public KeyPhrase(String phrase) {
        this(phrase, null);
    }

    public String getPhrase() {
        return this.phrase;
    }

    public void setPhrase(String phrase) {
        this.phrase = phrase;
    }

    public Double getWeighting() {
        return this.weighting;
    }

    public void setWeighting(Double weighting) {
        this.weighting = weighting;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof KeyPhrase) {
            KeyPhrase other = (KeyPhrase) that;
            return phrase.equals(other.phrase) 
                && Objects.equal(weighting, other.weighting);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(phrase, weighting);
    }
    
    @Override
    public String toString() {
        return String.format("%s (%s): %s", phrase, 
            weighting != null ? weighting : "unweighted");
    }
}
