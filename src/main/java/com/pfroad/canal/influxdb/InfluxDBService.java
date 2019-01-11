package com.pfroad.canal.influxdb;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.server.netty.ClientInstanceProfiler;
import com.alibaba.otter.canal.spi.CanalMetricsService;
import com.pfroad.canal.influxdb.binder.*;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.alibaba.otter.canal.server.netty.CanalServerWithNettyProfiler.NOP;
import static com.alibaba.otter.canal.server.netty.CanalServerWithNettyProfiler.profiler;

public class InfluxDBService implements CanalMetricsService {
    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBService.class);
//    private static final int STEP = 60 * 1000;
    private volatile boolean running = false;
    private MeterRegistry registry;
    private final ClientInstanceProfiler clientProfiler;
//    private final ScheduledExecutorService scheduledExecutorService;

    public InfluxDBService() {
        /**
         * metrics.influx:
         *  db:
         *  uri:
         *  userName:
         *  password:
         *  retentionPolicy:
         *  retentionDuration:
         *  retentionReplicationFactor:
         *  compressed:
         *  autoCreateDb:
         *  consistency:
         *  step:
         *  enabled:
         *  numThreads:
         *  connectTimeout:
         *  readTimeout:
         *  batchSize
         */
        InfluxConfig config = new InfluxConfig() {
            @Override
            public String prefix() {
                return "metrics.influx";
            }

            @Override
            public String get(String k) {
                return System.getProperty(k, System.getenv(k)); // accept the rest of the defaults
            }
        };

        this.registry = new InfluxMeterRegistry(config, Clock.SYSTEM);
        this.clientProfiler = new InfluxClientInstanceProfiler(registry);
    }

    private static class SingletonHolder {
        private static final InfluxDBService SINGLETON = new InfluxDBService();
    }

    public static CanalMetricsService getInstance() {
        return SingletonHolder.SINGLETON;
    }

    @Override
    public void initialize() {
        running = true;

        try {
            if (!clientProfiler.isStart()) {
                clientProfiler.start();
            }
            profiler().setInstanceProfiler(clientProfiler);
        } catch (Throwable e) {
            LOGGER.warn("Unable to initialize influx service.", e);
        }

    }

    @Override
    public void terminate() {
        running = false;

        try {
            if (clientProfiler.isStart()) {
                clientProfiler.stop();
            }

            profiler().setInstanceProfiler(NOP);

            if (!this.registry.isClosed()) {
                this.registry.close();
            }
        } catch (Throwable e) {
            LOGGER.warn("Something happened while terminating.", e);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void register(CanalInstance instance) {
        InetAddress inetAddress = null;
        try {
            inetAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
        }
        this.registry.config()
                .commonTags(Tags.of("app", "canal")
                .and(Tags.of("destination", instance.getDestination()))
                .and(Tags.of("host", inetAddress == null ? "canal" : inetAddress.getHostName())));

        registerDefaultMetrics(registry);
        registerCanalMetrics(registry, instance);
    }

    private void registerCanalMetrics(MeterRegistry registry, CanalInstance instance) {
        new EntryMetrics(instance).bindTo(registry);
        new MetaMetrics(instance).bindTo(registry);
        new ParserMetrics(instance).bindTo(registry);
        new SinkMetrics(instance).bindTo(registry);
        new StoreMetrics(instance).bindTo(registry);
    }

    private void registerDefaultMetrics(MeterRegistry registry) {
        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);
    }

    @Override
    public void unregister(CanalInstance instance) {

    }

    @Override
    public void setServerPort(int port) {

    }
}
