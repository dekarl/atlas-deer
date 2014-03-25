package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.entity.util.WriteException;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.atlasapi.schedule.ScheduleUpdateMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EquivalentScheduleStoreScheduleUpdateWorker extends BaseWorker<ScheduleUpdateMessage>{

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final EquivalentScheduleWriter scheduleWriter;

    public EquivalentScheduleStoreScheduleUpdateWorker(EquivalentScheduleWriter scheduleWriter) {
        this.scheduleWriter = checkNotNull(scheduleWriter);
    }

    @Override
    public void process(ScheduleUpdateMessage message) {
        try {
            scheduleWriter.updateSchedule(message.getScheduleUpdate());
        } catch (WriteException e) {
            log.error("update failed for " + message.getScheduleUpdate(), e);
        }
    }

}
