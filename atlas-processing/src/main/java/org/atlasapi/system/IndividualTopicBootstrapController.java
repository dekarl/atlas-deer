package org.atlasapi.system;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.topic.Topic;
import org.atlasapi.media.topic.TopicStore;
import org.atlasapi.media.topic.TopicWriter;
import org.atlasapi.persistence.topic.TopicLookupResolver;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

@Controller
public class IndividualTopicBootstrapController {

    private final TopicQueryResolver read;
    private final TopicWriter write;
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    public IndividualTopicBootstrapController(TopicQueryResolver read, TopicWriter write) {
        this.read = checkNotNull(read);
        this.write = checkNotNull(write);
    }
    
    @RequestMapping(value="/system/bootstrap/content/{id}", method=RequestMethod.POST)
    public void bootstrapTopic(@PathVariable("id") String encodedId,
            HttpServletResponse resp) throws IOException {
        Id id = Id.valueOf(idCodec.decode(encodedId));
        Maybe<Topic> possibleTopic = read.topicForId(id);
        
        if (possibleTopic.isNothing()) {
            resp.sendError(HttpStatusCode.NOT_FOUND.code());
            return;
        }
        
        write.writeTopic(possibleTopic.requireValue());
        resp.setStatus(HttpStatus.OK.value());
        resp.setContentLength(0);
    }
}
