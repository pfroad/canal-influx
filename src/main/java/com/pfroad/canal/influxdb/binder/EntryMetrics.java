package com.pfroad.canal.influxdb.binder;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.sink.CanalEventDownStreamHandler;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EntryMetrics implements MeterBinder {
    private static final Logger LOGGER = LoggerFactory.getLogger(EntryMetrics.class);
    private static final String DELAY = "canal.instance.traffic.delay";
    private static final String TRANSACTION = "canal.instance.transactions";
    private static final String DELAY_HELP = "Traffic delay of canal instance in milliseconds";
    private static final String TRANSACTION_HELP = "Transactions counter of canal instance";

    private final CanalInstance instance;

    public EntryMetrics(CanalInstance instance) {
        this.instance = instance;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        final CanalEventSink sink = instance.getEventSink();
        if (!(sink instanceof EntryEventSink)) {
            throw new IllegalArgumentException("CanalEventSink must be EntryEventSink");
        }
        final EntryEventSink entrySink = (EntryEventSink) sink;
        final InfluxCanalEventDownStreamHandler influxHandler = assembleHandler(entrySink);
        final long now = System.currentTimeMillis();
        Gauge.builder(DELAY, influxHandler, (handler) -> {
            final long latest = handler.getLatestExecuteTime().get();
            if (now >= latest) {
                return now - latest;
            }
            return 0;
        }).description(DELAY_HELP)
                .baseUnit("ms")
                .register(registry);

        FunctionCounter.builder(TRANSACTION, influxHandler, (handler) -> handler.getTransactionCounter().doubleValue())
                .description(TRANSACTION_HELP)
                .register(registry);

    }

    private InfluxCanalEventDownStreamHandler assembleHandler(EntryEventSink entrySink) {
        InfluxCanalEventDownStreamHandler ih = new InfluxCanalEventDownStreamHandler();
        List<CanalEventDownStreamHandler> handlers = entrySink.getHandlers();
        for (CanalEventDownStreamHandler handler : handlers) {
            if (handler instanceof InfluxCanalEventDownStreamHandler) {
                throw new IllegalStateException("InfluxCanalEventDownStreamHandler already exists in handlers.");
            }
        }
        entrySink.addHandler(ih, 0);
        return ih;
    }
}
