package com.pfroad.canal.influxdb.binder;

import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.server.netty.ClientInstanceProfiler;
import com.alibaba.otter.canal.server.netty.listener.ChannelFutureAggregator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class InfluxClientInstanceProfiler implements ClientInstanceProfiler {

    private static final String PACKET_TYPE    = "canal.instance.client.packets";
    private static final String OUTBOUND_BYTES = "canal.instance.client.bytes";
    private static final String EMPTY_BATCHES  = "canal.instance.client.empty.batches";
    private static final String ERRORS         = "canal.instance.client.request.error";
    private static final String LATENCY        = "canal.instance.client.request.latency";
    private static final String OUTBOUND_COUNTER = "outbound_counter";
    private static final String PACKETS_COUNTER = "packets_counter";
    private static final String EMPTY_COUNTER = "empty_counter";
    private static final String ERROR_COUNTER = "errors_counter";
    private static final String LATENCY_GAUGE = "latency_gauge";

//    private Counter outboundCounter;
//    private Counter packetsCounter;
//    private Counter emptyBatchCounter;
//    private Counter errorsCounter;
//    private Gauge responseLatency;
    private final MeterRegistry registry;
    private final AtomicLong latency = new AtomicLong(0);
    private boolean running = false;

    private final ConcurrentMap<String, AtomicLong> latencyMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Meter> meters = new ConcurrentHashMap<>(16);

    public InfluxClientInstanceProfiler(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void profiling(ChannelFutureAggregator.ClientRequestResult result) {
        final String destination = result.getDestination();
        final CanalPacket.PacketType type = result.getType();

        countOutbound(destination, result.getAmount());

        final short errorCode = result.getErrorCode();
        if (errorCode > 0) {
            countErrors(destination, errorCode);
        }

        latencyGauge(destination, result.getLatency() / 1000 * 1000L);

        switch (type) {
            case GET:
                final boolean empty = result.getEmpty();
                if (empty) {
                    incEmptyBatches(destination);
//                    emptyBatchCounter.increment();
                } else {
                    incPackets(destination);
//                    packetsCounter.increment();
                }
                break;
            default:
                incPackets(destination);
                break;
        }
    }

    private void countOutbound(String destination, int amount) {
        final String mKey = destination + "_" + OUTBOUND_COUNTER;
        if (!meters.containsKey(mKey)) {
            final Counter outboundCounter = Counter.builder(OUTBOUND_BYTES)
                    .tag("destination", destination)
                    .description("Total bytes sent to client.")
                    .baseUnit("bytes")
                    .register(this.registry);
            meters.putIfAbsent(mKey, outboundCounter);
        }

        ((Counter) meters.get(mKey)).increment(amount);
    }

    private void countErrors(String destination, short errorCode) {
        final String mKey = destination + "_" + ERROR_COUNTER;
        if (!meters.containsKey(mKey)) {
            final Counter errorsCounter = Counter.builder(ERRORS)
                    .tag("destination", destination)
                    .tag("errorCode", String.valueOf(errorCode))
                    .description("Total client request errors.")
                    .baseUnit("errors")
                    .register(registry);

            meters.putIfAbsent(mKey, errorsCounter);
        }

        ((Counter) meters.get(mKey)).increment();
    }

    private void incPackets(String destination) {
        final String mKey = destination + "_" + PACKETS_COUNTER;
        if (!meters.containsKey(mKey)) {
            final Counter packetsCounter = Counter.builder(PACKET_TYPE)
                    .tag("destination", destination)
                    .description("Total packets sent to client.")
                    .baseUnit("packets")
                    .register(registry);

            meters.putIfAbsent(mKey, packetsCounter);
        }

        ((Counter) meters.get(mKey)).increment();
    }

    private void incEmptyBatches(String destination) {
        final String mKey = destination + "_" + EMPTY_COUNTER;
        if (!meters.containsKey(mKey)) {
            final Counter emptyBatchCounter = Counter.builder(EMPTY_BATCHES)
                    .tag("destination", destination)
                    .description("Total empty batches sent to client.")
                    .baseUnit("batches")
                    .register(registry);
            meters.putIfAbsent(mKey, emptyBatchCounter);
        }

        ((Counter) meters.get(mKey)).increment();
    }

    private void latencyGauge(String destination, long latency) {
        final String mKey = destination + "_" + LATENCY_GAUGE;
        if (!latencyMap.containsKey(destination)) {
            latencyMap.putIfAbsent(destination, new AtomicLong(0));
        }

        if (!meters.containsKey(mKey)) {
            final Gauge responseLatency = Gauge.builder(LATENCY, latencyMap.get(destination), (l) -> l.doubleValue())
                    .tag("destination", destination)
                    .description("Client request latency.")
                    .baseUnit("ms")
                    .register(registry);
            meters.putIfAbsent(mKey, responseLatency);
        }

        latencyMap.get(destination).getAndSet(latency);
    }

    @Override
    public void start() {
//        outboundCounter = Counter.builder(OUTBOUND_BYTES)
//                .description("Total bytes sent to client.")
//                .baseUnit("bytes")
//                .register(this.registry);
//        packetsCounter = Counter.builder(PACKET_TYPE)
//                .description("Total packets sent to client.")
//                .baseUnit("packets")
//                .register(registry);
//        emptyBatchCounter = Counter.builder(EMPTY_BATCHES)
//                .description("Total empty batches sent to client.")
//                .baseUnit("batches")
//                .register(registry);
//        errorsCounter = Counter.builder(ERRORS)
//                .description("Total client request errors.")
//                .baseUnit("errors")
//                .register(registry);
//        responseLatency = Gauge.builder(LATENCY, latency, (l) -> l.doubleValue())
//                .description("Client request latency.")
//                .baseUnit("ms")
//                .register(registry);
        running = true;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isStart() {
        return running;
    }
}
