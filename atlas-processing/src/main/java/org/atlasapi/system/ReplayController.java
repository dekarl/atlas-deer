package org.atlasapi.system;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.metabroadcast.common.webapp.query.DateTimeInQueryParser;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;
import org.atlasapi.messaging.producers.MessageReplayer;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 */
@Controller
public class ReplayController {

    private final DateTimeInQueryParser dateTimeParser = new DateTimeInQueryParser();
    private final MessageReplayer messageReplayer;

    public ReplayController(MessageReplayer messageReplayer) {
        this.messageReplayer = messageReplayer;
    }

    @RequestMapping(method = RequestMethod.POST, value = "/system/replay")
    public void replay(
           @RequestParam(required = true) String from, 
           @RequestParam(required = true) String to, 
           @RequestParam(required = true) String destination, 
           @RequestParam(required = false) String source,
           HttpServletResponse response) throws IOException {
        if (Strings.isNullOrEmpty(from)
                || Strings.isNullOrEmpty(to)
                || Strings.isNullOrEmpty(destination)) {
            throw new IllegalArgumentException("Request parameters 'destination', 'from' and 'to' are required!");
        }

        messageReplayer.replay(destination, dateTimeParser.parse(from), dateTimeParser.parse(to), Optional.fromNullable(Strings.emptyToNull(source)));
    }
}
