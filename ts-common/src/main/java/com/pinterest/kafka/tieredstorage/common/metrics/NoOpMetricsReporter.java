package com.pinterest.kafka.tieredstorage.common.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A No-Op MetricsReporter that does nothing
 */
public class NoOpMetricsReporter extends MetricsReporter {
    public NoOpMetricsReporter(MetricsConfiguration metricsConfiguration, MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, String serializedMetricTagString) {
        super(metricsConfiguration, registry, name, filter, rateUnit, durationUnit, serializedMetricTagString);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        // no-op
    }
}
