package com.pfroad.canal.influxdb.binder;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class ParserMetrics implements MeterBinder {
    private final static Logger LOGGER = LoggerFactory.getLogger(ParserMetrics.class);

    private static final String PUBLISH_BLOCKING = "canal.instance.publish.blocking.time";
    private static final String RECEIVED_BINLOG  = "canal.instance.received.binlog.bytes";
    private static final String PARSER_MODE      = "canal.instance.parser.mode";
    private static final String MODE_LABEL            = "parallel";
    private static final String PUBLISH_BLOCKING_HELP = "Publish blocking time of dump thread in milliseconds";
    private static final String RECEIVED_BINLOG_HELP  = "Received binlog bytes";
    private static final String MODE_HELP             = "Parser mode(parallel/serial) of instance";

    private final CanalInstance instance;

    public ParserMetrics(CanalInstance instance) {
        this.instance = instance;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        final CanalEventParser parser = instance.getEventParser();
        if (!(parser instanceof MysqlEventParser)) {
            throw new IllegalArgumentException("CanalEventParser must be MysqlEventParser");
        }
        final MysqlEventParser mysqlEventParser = (MysqlEventParser) parser;

        FunctionCounter.builder(PUBLISH_BLOCKING, mysqlEventParser,
                (mep) -> mep.getEventsPublishBlockingTime().doubleValue() / 1000 * 1000L)
                .description(PUBLISH_BLOCKING_HELP)
                .baseUnit("ms")
                .register(registry);
        FunctionCounter.builder(RECEIVED_BINLOG, mysqlEventParser, (mep) -> mep.getReceivedBinlogBytes().doubleValue())
                .description(RECEIVED_BINLOG_HELP)
                .baseUnit("bytes")
                .register(registry);
        Gauge.builder(PARSER_MODE, new AtomicInteger(), (ai) -> 1)
//                .tag("destination", instance.getDestination())
                .tag("parallel", mysqlEventParser.isParallel() ? "true" : "false")
                .description(MODE_HELP)
                .register(registry);
    }
}
