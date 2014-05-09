package org.atlasapi.system.bootstrap.workers;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.system.bootstrap.ChannelIntervalScheduleBootstrapTask;
import org.atlasapi.system.bootstrap.SourceChannelIntervalFactory;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class ScheduleReadWriteWorker implements Worker<ScheduleUpdateMessage> {

    private final Logger log = LoggerFactory.getLogger(ScheduleReadWriteWorker.class);

    private final SubstitutionTableNumberCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final SourceChannelIntervalFactory<ChannelIntervalScheduleBootstrapTask> taskFactory;
    private final ChannelResolver channelResolver;

    public ScheduleReadWriteWorker(SourceChannelIntervalFactory<ChannelIntervalScheduleBootstrapTask> taskFactory, 
            ChannelResolver channelResolver) {
        this.channelResolver = checkNotNull(channelResolver);
        this.taskFactory = checkNotNull(taskFactory);
    }
    
    @Override
    public void process(ScheduleUpdateMessage msg) {
        String updateMsg = String.format("update %s %s %s-%s", 
                msg.getSource(), msg.getChannel(), msg.getUpdateStart(), msg.getUpdateEnd());
        
        Maybe<Publisher> source = Publisher.fromKey(msg.getSource());
        if (!source.hasValue()) {
            log.warn(updateMsg + ": unknown source %s", msg.getSource());
            return;
        }
        
        long cid = idCodec.decode(msg.getChannel()).longValue();
        Maybe<Channel> channel = channelResolver.fromId(cid);
        if (!channel.hasValue()) {
            log.warn(updateMsg + ": unknown channel %s (%s)", msg.getChannel(), cid);
        }
        
        Interval interval = new Interval(msg.getUpdateStart(), msg.getUpdateEnd());
        
        log.debug(updateMsg + ": processing");
        try {
            UpdateProgress result = taskFactory.create(source.requireValue(), channel.requireValue(), interval).call();
            log.debug(updateMsg + ": processed: " + result);
        } catch (Exception e) {
            log.error("failed " + updateMsg, e);
        }
    }

}
