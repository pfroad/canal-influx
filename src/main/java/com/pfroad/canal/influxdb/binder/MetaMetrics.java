package com.pfroad.canal.influxdb.binder;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.spring.CanalInstanceWithSpring;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaMetrics implements MeterBinder {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaMetrics.class);

    private static final String INSTANCE          = "canal.instance";
    private static final String INSTANCE_HELP     = "Canal instance";
    private static final String SUBSCRIPTION      = "canal.instance.subscriptions";
    private static final String SUBSCRIPTION_HELP = "Canal instance subscriptions";

    private final CanalInstance instance;

    public MetaMetrics(CanalInstance instance) {
        this.instance = instance;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        final CanalMetaManager metaManager = instance.getMetaManager();
        Preconditions.checkNotNull(metaManager);
        final String destination = instance.getDestination();

        Gauge.builder(INSTANCE, new AtomicInteger(0), (ai) -> 1)
//                .tag("destination", destination)
                .tag("mode", (instance instanceof CanalInstanceWithSpring) ? "spring" : "manager")
                .description(INSTANCE_HELP)
                .register(registry);
        Gauge.builder(SUBSCRIPTION, metaManager, (meta) -> {
            final List<ClientIdentity> subs = meta.listAllSubscribeInfo(destination);
            return subs == null ? 0 : ((List) subs).size();
        }).description(SUBSCRIPTION_HELP)
                .baseUnit("subs")
                .register(registry);
    }
}
