package com.pfroad.canal.influxdb.binder;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkMetrics implements MeterBinder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkMetrics.class);

    private static final String SINK_BLOCKING_TIME   = "canal.instance.sink.blocking.time";
    private static final String SINK_BLOCK_TIME_HELP = "Total sink blocking time in milliseconds";
    private final CanalInstance instance;

    public SinkMetrics(CanalInstance instance) {
        this.instance = instance;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        final CanalEventSink sink = instance.getEventSink();
        if (!(sink instanceof EntryEventSink)) {
            throw new IllegalArgumentException("CanalEventSink must be EntryEventSink");
        }
        EntryEventSink entryEventSink = (EntryEventSink) sink;
        FunctionCounter.builder(SINK_BLOCKING_TIME, entryEventSink, (eventSink) -> eventSink.getEventsSinkBlockingTime().doubleValue() / 1000 * 1000L)
                .description(SINK_BLOCK_TIME_HELP)
                .baseUnit("ms")
                .register(registry);
    }
}
