package org.atlasapi.query.common;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;


public class QueryRequestParameterValidator extends AbstractRequestParameterValidator {

    private final PrefixInTree<Boolean> attributeParameters;
    private final ImmutableSet<String> contextParameters;
    private final ReplacementSuggestion replacementSuggestion;

    public QueryRequestParameterValidator(QueryAttributeParser attributeParser,
            ImmutableSet<String> otherParameters) {
        this.attributeParameters = initAttributeParams(attributeParser);
        this.contextParameters = ImmutableSet.copyOf(otherParameters);
        this.replacementSuggestion = new ReplacementSuggestion(allParams(), "Invalid parameters: ", " (did you mean %s?)");
    }
    
    private Iterable<String> allParams() {
        return ImmutableSet.copyOf(Iterables.concat(
                attributeParameters.allKeys(), contextParameters));
    }

    private PrefixInTree<Boolean> initAttributeParams(QueryAttributeParser attributeParser) {
        PrefixInTree<Boolean> attributeParams = new PrefixInTree<Boolean>();
        Optional<Boolean> value = Optional.of(Boolean.TRUE);
        for (String validKeyPrefix : attributeParser.getParameterNames()) {
            attributeParams.put(validKeyPrefix, value);
        }
        return attributeParams;
    }

    @Override
    protected List<String> determineInvalidParameters(Set<String> requestParams) {
        LinkedList<String> invalid = Lists.newLinkedList();
        for (String requestParam : requestParams) {
            if (!(isContextParam(requestParam) || isAttributeParam(requestParam)))  {
                invalid.add(requestParam);
            }
        }
        return invalid;
    }

    private boolean isAttributeParam(String requestParam) {
        return attributeParameters.valueForKeyPrefixOf(requestParam).isPresent();
    }

    private boolean isContextParam(String requestParam) {
        return contextParameters.contains(requestParam);
    }

    @Override
    protected Set<String> determineMissingParameters(Set<String> requestParams) {
        return ImmutableSet.of();
    }

    @Override
    protected String invalidParameterMessage(Collection<String> invalidParams) {
        return replacementSuggestion.forInvalid(invalidParams);
    }

    @Override
    protected String missingParameterMessage(Collection<String> missingParams) {
        return "";
    }

}
