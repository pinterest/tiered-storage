package com.pinterest.kafka.tieredstorage.common.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * OpenTSDBReporter is a MetricsReporter that reports metrics to OpenTSDB
 */
public class OpenTSDBReporter extends MetricsReporter {

    private static final AtomicReference<OpenTSDBClient> openTSDBClient = new AtomicReference<>();
    public OpenTSDBReporter(MetricsConfiguration metricsConfiguration,
                            MetricRegistry registry,
                            String name,
                            MetricFilter filter,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            String serializedMetricTagString)  {
        super(metricsConfiguration, registry, name, filter, rateUnit, durationUnit, serializedMetricTagString);
        openTSDBClient.set(OpenTSDBClient.getInstance(metricsConfiguration));
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        for (@SuppressWarnings("rawtypes") Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            if (entry.getValue().getValue() instanceof Long) {
                openTSDBClient.get().sendMetric(entry.getKey(), (Long) entry.getValue().getValue(), serializedMetricTagString);
            } else if (entry.getValue().getValue() instanceof Double) {
                openTSDBClient.get().sendMetric(entry.getKey(), (Double) entry.getValue().getValue(), serializedMetricTagString);
            }
        }
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            openTSDBClient.get().sendMetric(entry.getKey(), entry.getValue().getCount(), serializedMetricTagString);
        }
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            generateMetrics(entry.getKey(), entry.getValue().getSnapshot());
        }
        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            generateMetrics(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            generateMetrics(entry.getKey(), entry.getValue().getSnapshot());
        }
    }

    private void generateMetrics(String key, Meter meter) {
        openTSDBClient.get().sendMetric(key + ".meanRate", meter.getMeanRate(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".count", meter.getCount(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".oneMinRate", meter.getOneMinuteRate(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".fiveMinRate", meter.getFiveMinuteRate(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".fifteenMinuteRate", meter.getFifteenMinuteRate(), serializedMetricTagString);
    }

    private void generateMetrics(String key, Snapshot snapshot) {
        openTSDBClient.get().sendMetric(key + ".avg", snapshot.getMean(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".min", snapshot.getMin(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".median", snapshot.getMedian(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".p50", snapshot.getMedian(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".p75", snapshot.get75thPercentile(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".p95", snapshot.get95thPercentile(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".p98", snapshot.get98thPercentile(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".p99", snapshot.get99thPercentile(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".p999", snapshot.get999thPercentile(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".max", snapshot.getMax(), serializedMetricTagString);
        openTSDBClient.get().sendMetric(key + ".stddev", snapshot.getStdDev(), serializedMetricTagString);
    }
}
