package com.pfroad.canal.influxdb;

import com.alibaba.otter.canal.spi.CanalMetricsProvider;
import com.alibaba.otter.canal.spi.CanalMetricsService;

public class InfluxDBProvider implements CanalMetricsProvider {
    @Override
    public CanalMetricsService getService() {
        return InfluxDBService.getInstance();
    }
}