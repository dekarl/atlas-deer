package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EquivalentScheduleStoreGraphUpdateWorker extends
        BaseWorker<EquivalenceGraphUpdateMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final EquivalentScheduleWriter scheduleWriter;

    public EquivalentScheduleStoreGraphUpdateWorker(EquivalentScheduleWriter scheduleWriter) {
        this.scheduleWriter = checkNotNull(scheduleWriter);
    }

    @Override
    public void process(EquivalenceGraphUpdateMessage message) {
        try {
            scheduleWriter.updateEquivalences(message.getGraphUpdate());
        } catch (WriteException e) {
            log.error("update failed for " + message.getGraphUpdate(), e);
        }
    }

}
