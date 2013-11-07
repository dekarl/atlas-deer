package org.atlasapi.query.common;

import java.util.Collection;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class SetBasedRequestParameterValidator extends AbstractRequestParameterValidator {
    
    public static final Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        
        private ImmutableSet<String> required;
        private ImmutableSet<String> optional;

        public Builder withRequiredParameters(String...parameters) {
            this.required = ImmutableSet.copyOf(parameters);
            return this;
        }
        
        public Builder withOptionalParameters(String...parameters) {
            this.optional = ImmutableSet.copyOf(parameters);
            return this;
        }
        
        public SetBasedRequestParameterValidator build() {
            return new SetBasedRequestParameterValidator(required, optional);
        }
    }
    
    public SetBasedRequestParameterValidator(ParameterNameProvider... nameProviders) {
        this(ImmutableSet.<String>of(), ImmutableSet.copyOf(Iterables.concat(Iterables.transform(
            ImmutableList.copyOf(nameProviders),
            new Function<ParameterNameProvider, Set<String>>() {
                @Override
                public Set<String> apply(ParameterNameProvider input) {
                    return input.getParameterNames();
                }
            }
        ))));
    }
    
    private final Set<String> requiredParams;
    private final Set<String> optionalParams;
    
    private final Set<String> allParams;
    private final String validParamMsg;
    private final ReplacementSuggestion replacementSuggestion;

    private SetBasedRequestParameterValidator(Set<String> requiredParams, Set<String> optionalParams) {
        this.requiredParams = ImmutableSet.copyOf(requiredParams);
        this.optionalParams = ImmutableSet.copyOf(optionalParams);
        this.allParams = ImmutableSet.copyOf(Sets.union(this.requiredParams, this.optionalParams));
        this.validParamMsg = "Valid params: " + commaJoiner.join(allParams);
        this.replacementSuggestion = new ReplacementSuggestion(allParams, "Invalid parameters: ", " (did you mean %s?)");
    }

    @Override
    protected Set<String> determineMissingParameters(Set<String> requestParams) {
        return Sets.difference(requiredParams, requestParams);
    }

    @Override
    protected Set<String> determineInvalidParameters(Set<String> requestParams) {
        return Sets.difference(requestParams, allParams);
    }

    @Override
    protected String missingParameterMessage(Collection<String> missingParams) {
        return String.format("Missing parameters: %s.", commaJoiner.join(missingParams));
    }

    @Override
    protected String invalidParameterMessage(Collection<String> invalidParams) {
        return String.format("%s. %s.", replacementSuggestion.forInvalid(invalidParams), validParamMsg);
    }

}
