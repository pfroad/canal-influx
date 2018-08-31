package com.pfroad.canal.influxdb.binder;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreMetrics implements MeterBinder {
    private static final Logger LOGGER = LoggerFactory.getLogger(StoreMetrics.class);

    private static final String PRODUCE          = "canal.instance.store.produce.seq";
    private static final String CONSUME          = "canal.instance.store.consume.seq";
    private static final String STORE            = "canal.instance.store";
    private static final String PRODUCE_MEM      = "canal.instance.store.produce.mem";
    private static final String CONSUME_MEM      = "canal.instance.store.consume.mem";
    private static final String PUT_DELAY        = "canal.instance.put.delay";
    private static final String GET_DELAY        = "canal.instance.get.delay";
    private static final String ACK_DELAY        = "canal.instance.ack.delay";
    private static final String PUT_ROWS         = "canal.instance.put.rows";
    private static final String GET_ROWS         = "canal.instance.get.rows";
    private static final String ACK_ROWS         = "canal.instance.ack.rows";
    private static final String PRODUCE_HELP     = "Produced events counter of canal instance";
    private static final String CONSUME_HELP     = "Consumed events counter of canal instance";
    private static final String STORE_HELP       = "Canal instance info";
    private static final String PRODUCE_MEM_HELP = "Produced mem bytes of canal instance";
    private static final String CONSUME_MEM_HELP = "Consumed mem bytes of canal instance";
    private static final String PUT_DELAY_HELP   = "Traffic delay of canal instance put";
    private static final String GET_DELAY_HELP   = "Traffic delay of canal instance get";
    private static final String ACK_DELAY_HELP   = "Traffic delay of canal instance ack";
    private static final String PUT_ROWS_HELP    = "Put table rows of canal instance";
    private static final String GET_ROWS_HELP    = "Got table rows of canal instance";
    private static final String ACK_ROWS_HELP    = "Acked table rows of canal instance";

    private final CanalInstance instance;

    public StoreMetrics(CanalInstance instance) {
        this.instance = instance;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        final CanalEventStore store = this.instance.getEventStore();
        if (!(store instanceof MemoryEventStoreWithBuffer)) {
            throw new IllegalArgumentException("EventStore must be MemoryEventStoreWithBuffer");
        }
        final MemoryEventStoreWithBuffer memStore = (MemoryEventStoreWithBuffer) store;

        FunctionCounter.builder(PRODUCE, memStore, (mem) -> mem.getPutSequence().doubleValue())
                .description(PRODUCE_HELP)
//                .baseUnit("seq")
                .register(registry);
//        final Counter produceEvents = Counter.builder(PRODUCE)
//                .description(PRODUCE_HELP)
//                .register(registry);
        FunctionCounter.builder(CONSUME, memStore, (mem) -> mem.getAckSequence().doubleValue())
                .description(CONSUME_HELP)
//                .baseUnit("seq")
                .register(registry);
//        final Counter consumerEvents = Counter.builder(CONSUME)
//                .description(CONSUME_HELP)
//                .register(registry);
//        final List<String> storeLabelValues = Arrays.asList(instance.getDestination(), )
        Gauge.builder(STORE, memStore, MemoryEventStoreWithBuffer::getBufferSize)
                .tag("batchMode", memStore.getBatchMode().name())
                .description(STORE_HELP)
                .register(registry);

        final boolean isMem = memStore.getBatchMode().isMemSize();
        if (isMem) {
            FunctionCounter.builder(PRODUCE_MEM, memStore, (mem) -> mem.getPutMemSize().doubleValue())
                    .description(PRODUCE_MEM_HELP)
                    .baseUnit("bytes")
                    .register(registry);
            FunctionCounter.builder(CONSUME_MEM, memStore, (mem) -> mem.getAckMemSize().doubleValue())
                    .description(CONSUME_MEM_HELP)
                    .baseUnit("bytes")
                    .register(registry);
        }
//        final Counter produceMem = Counter.builder(PRODUCE_MEM)
//                .description(PRODUCE_MEM_HELP)
//                .register(registry);
//        final Counter consumeMem = Counter.builder(CONSUME_MEM)
//                .description(CONSUME_MEM_HELP)
//                .register(registry);

        final long now = System.currentTimeMillis();
        Gauge.builder(PUT_DELAY, memStore, (mem) -> {
            final long put = mem.getPutExecTime().get();
            if (now >= put) {
                return now - put;
            }
            return 0;
        }).description(PUT_DELAY_HELP)
                .baseUnit("ms")
                .register(registry);
        Gauge.builder(GET_DELAY, memStore, (mem) -> {
            final long put = mem.getPutExecTime().get();
            final long get = Math.min(mem.getGetExecTime().get(), put);
            if (now >= get) {
                return now - get;
            }
            return 0;
        }).description(GET_DELAY_HELP)
                .baseUnit("ms")
                .register(registry);
        Gauge.builder(ACK_DELAY, memStore, (mem) -> {
            final long get = mem.getGetExecTime().get();
            final long ack = Math.min(mem.getAckExecTime().get(), get);
            if (now >= ack) {
                return now - ack;
            }
            return 0;
        }).description(ACK_DELAY_HELP)
                .baseUnit("ms")
                .register(registry);

        FunctionCounter.builder(PUT_ROWS, memStore, (mem) -> mem.getPutTableRows().doubleValue())
                .description(PUT_ROWS_HELP)
                .baseUnit("rows")
                .register(registry);
        FunctionCounter.builder(GET_ROWS, memStore, (mem) -> mem.getGetTableRows().doubleValue())
                .baseUnit("rows")
                .description(GET_ROWS_HELP)
                .register(registry);
        FunctionCounter.builder(ACK_ROWS, memStore, (mem) -> mem.getAckTableRows().doubleValue())
                .description(ACK_ROWS_HELP)
                .baseUnit("rows")
                .register(registry);
//        final Counter putRows = Counter.builder(PUT_ROWS)
//                .description(PUT_ROWS_HELP)
//                .register(registry);
//        final Counter getRows = Counter.builder(GET_ROWS)
//                .description(GET_ROWS_HELP)
//                .register(registry);
//        final Counter ackRows = Counter.builder(ACK_ROWS)
//                .description(ACK_ROWS_HELP)
//                .register(registry);
    }
}
