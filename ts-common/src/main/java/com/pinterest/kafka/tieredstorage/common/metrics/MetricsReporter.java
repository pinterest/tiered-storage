package com.pinterest.kafka.tieredstorage.common.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Represents an abstract MetricsReporter
 */
public abstract class MetricsReporter extends ScheduledReporter {

    protected final MetricsConfiguration metricsConfiguration;
    protected final String serializedMetricTagString;

    protected MetricsReporter(MetricsConfiguration metricsConfiguration,
                              MetricRegistry registry,
                              String name,
                              MetricFilter filter,
                              TimeUnit rateUnit,
                              TimeUnit durationUnit,
                              String serializedMetricTagString) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.metricsConfiguration = metricsConfiguration;
        this.serializedMetricTagString = serializedMetricTagString;
    }

    @Override
    public abstract void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers);
}
