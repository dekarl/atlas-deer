package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.http.HttpStatusCode.BAD_REQUEST;
import static com.metabroadcast.common.http.HttpStatusCode.SERVER_ERROR;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.elasticsearch.common.base.Throwables;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DateTimeZones;

@Controller
public class ScheduleBootstrapController {
    
    private final ChannelIntervalScheduleBootstrapTaskFactory taskFactory;
    private final ChannelResolver channelResvoler;
    
    private static final DateTimeFormatter dateParser = ISODateTimeFormat.date();
    private static final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    public ScheduleBootstrapController(ChannelIntervalScheduleBootstrapTaskFactory taskFactory, ChannelResolver channelResvoler) {
        this.taskFactory = checkNotNull(taskFactory);
        this.channelResvoler = checkNotNull(channelResvoler);
    }
    
    @RequestMapping(value="/system/bootstrap/schedule",method=RequestMethod.POST)
    public Void bootstrapSchedule(HttpServletResponse resp, @RequestParam("source") String src,
            @RequestParam("day") String day, @RequestParam("channel") String channelId)
        throws IOException {
        
        Maybe<Publisher> source = Publisher.fromKey(src);
        if (!source.hasValue()) {
            return failure(resp, BAD_REQUEST, "Unknown source " + src);
        }
        
        Maybe<Channel> channel = resolve(channelId);
        if (!channel.hasValue()) {
            return failure(resp, BAD_REQUEST, "Unknown channel " + channelId);
        }
        
        LocalDate date;
        try {
            date = dateParser.parseLocalDate(day);
        } catch (IllegalArgumentException iae) {
            return failure(resp, BAD_REQUEST, "Failed to parse "+day+", expected yyyy-MM-dd");
        }
        
        try {
            UpdateProgress progress = taskFactory.create(source.requireValue(), channel.requireValue(), interval(date)).call();
            resp.setStatus(HttpStatusCode.OK.code());
            resp.getWriter().write(progress.toString());
            return null;
        } catch (Exception e) {
            return failure(resp, SERVER_ERROR, Throwables.getStackTraceAsString(e));
        }
        
    }

    private Interval interval(LocalDate day) {
        return new Interval(day.toDateTimeAtStartOfDay(DateTimeZones.UTC),
                day.plusDays(1).toDateTimeAtStartOfDay(DateTimeZones.UTC));
    }

    private Maybe<Channel> resolve(String channelId) {
        return channelResvoler.fromId(idCodec.decode(channelId).longValue());
    }

    private Void failure(HttpServletResponse resp, HttpStatusCode status, String msg) throws IOException {
        resp.setStatus(status.code());
        resp.getWriter().write(msg);
        return null;
    }
    
}
