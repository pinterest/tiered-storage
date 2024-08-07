package com.pinterest.kafka.tieredstorage.common.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

/**
 * MetricRegistryAndReporter is a wrapper class that contains a MetricRegistry and a ScheduledReporter
 */
public class MetricRegistryAndReporter {
    private final MetricRegistry metricRegistry;
    private final ScheduledReporter reporter;

    public MetricRegistryAndReporter(MetricRegistry registry, ScheduledReporter reporter) {
        this.metricRegistry = registry;
        this.reporter = reporter;
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public ScheduledReporter getReporter() {
        return reporter;
    }
}
