package org.atlasapi.query.common.useraware;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.query.common.AbstractRequestParameterValidator;
import org.atlasapi.query.common.QueryAttributeParser;
import org.atlasapi.query.common.QueryParseException;
import org.atlasapi.query.common.QueryRequestParameterValidator;
import org.atlasapi.query.common.Resource;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class StandardUserAwareQueryParser<T> implements UserAwareQueryParser<T> {

    private final NumberToShortStringCodec idCodec;
    private final QueryAttributeParser attributeParser;
    private final UserAwareQueryContextParser contextParser;

    private final Pattern singleResourcePattern;
    private final AbstractRequestParameterValidator parameterValidator;

    public StandardUserAwareQueryParser(Resource resource, QueryAttributeParser attributeParser,
                            NumberToShortStringCodec idCodec,
                            UserAwareQueryContextParser contextParser) {
        this.parameterValidator = new QueryRequestParameterValidator(attributeParser, contextParser);
        this.attributeParser = checkNotNull(attributeParser);
        this.contextParser = checkNotNull(contextParser);
        this.idCodec = checkNotNull(idCodec);
        this.singleResourcePattern = Pattern.compile(resource.getPlural() + "/([^.]+)(\\..*)?$");
    }

    @Override
    public UserAwareQuery<T> parse(HttpServletRequest request) throws QueryParseException, InvalidApiKeyException {
        parameterValidator.validateParameters(request);
        Id singleId = tryExtractSingleId(request);
        return singleId != null ? singleQuery(request, singleId) 
                                : listQuery(request);
    }

    private Id tryExtractSingleId(HttpServletRequest request) {
        Matcher matcher = singleResourcePattern.matcher(request.getRequestURI());
        return matcher.find() ? Id.valueOf(idCodec.decode(matcher.group(1)))
                              : null;
    }
    
    private UserAwareQuery<T> singleQuery(HttpServletRequest request, Id singleId) throws QueryParseException, InvalidApiKeyException {
        return UserAwareQuery.singleQuery(singleId, contextParser.parseSingleContext(request));
    }

    private UserAwareQuery<T> listQuery(HttpServletRequest request) throws QueryParseException, InvalidApiKeyException {
        AttributeQuerySet querySet = attributeParser.parse(request);
        return UserAwareQuery.listQuery(querySet,
            contextParser.parseListContext(request));
    }
    
}
