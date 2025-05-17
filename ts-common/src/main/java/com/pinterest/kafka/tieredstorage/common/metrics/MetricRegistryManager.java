package com.pinterest.kafka.tieredstorage.common.metrics;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.google.common.annotations.VisibleForTesting;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages MetricRegistry and ScheduledReporter instances for different topics and partitions.
 * This class is accessed via a singleton instance with {@link #getInstance(MetricsConfiguration)}and is thread-safe.
 * The provided {@link MetricsConfiguration} specifies the {@link MetricsReporter} implementation that should be used
 * for metrics reporting and emission.
 * <p>
 * Each topic, partition, and additional tags combination is associated with a unique pair of {@link MetricRegistry} and
 * {@link MetricsReporter} instances, wrapped in a {@link MetricRegistryAndReporter} object.
 */
public class MetricRegistryManager {

    private static final Logger LOG = LogManager.getLogger(MetricRegistryManager.class.getName());
    private static final ThreadLocal<MetricRegistryManager> metricRegistryManager = ThreadLocal.withInitial(() -> null);
    private final Map<String, MetricRegistryAndReporter> metricRegistryAndReporterMap;
    private ScheduledExecutorService executorService;
    private static final AtomicInteger refCount = new AtomicInteger(0);
    private final MetricsConfiguration metricsConfiguration;
    private static final long REPORTER_FREQUENCY_MS = 60000;  // TODO: make this configurable

    private MetricRegistryManager(MetricsConfiguration metricsConfiguration) {
        LOG.info("Initializing MetricRegistryManager with configuration:  " + metricsConfiguration);
        this.metricsConfiguration = metricsConfiguration;
        this.metricRegistryAndReporterMap = new ConcurrentHashMap<>();
        if (!metricsConfiguration.getMetricsReporterClassName().equals(NoOpMetricsReporter.class.getName())) {
            // initialize the thread pool only once
            if (refCount.getAndIncrement() == 0)
                executorService = Executors.newScheduledThreadPool(5);  // TODO: make this configurable

        }
    }

    /**
     * Shuts down the executorService and clears the metricRegistryAndReporter map if the refCount is 0.
     */
    public void shutdown() throws InterruptedException {
        if (refCount.decrementAndGet() == 0) {
            LOG.info("Shutting down executorService and clearing metricRegistryAndReporter map");
            executorService.shutdown();
            metricRegistryAndReporterMap.clear();
        }
        metricRegistryManager.set(null);
    }

    /**
     * Updates the histogram with the given metricValue for the given topic, partition, and metricName.
     * @param topic the topic
     * @param partition the partition
     * @param metricName the metric name
     * @param metricValue the metric value
     * @param additionalTags the additional tags
     */
    public void updateHistogram(String topic, Integer partition, String metricName, long metricValue, String... additionalTags) {
        if (metricsConfiguration.getMetricsReporterClassName().equals(NoOpMetricsReporter.class.getName())) {
            return;
        }
        MetricRegistryAndReporter metricRegistryAndReporter = getOrCreateMetricRegistryAndReporter(topic, partition, additionalTags);
        metricRegistryAndReporter.getMetricRegistry().histogram(metricName).update(metricValue);
    }

    /**
     * Increments the counter for the given topic, partition, and metricName by 1.
     * @param topic the topic
     * @param partition the partition
     * @param metricName the metric name
     * @param additionalTags the additional tags
     */
    public void incrementCounter(String topic, Integer partition, String metricName, String... additionalTags) {
        incrementCounter(topic, partition, metricName, 1L, additionalTags);
    }

    /**
     * Increments the counter for the given topic, partition, and metricName by the given value.
     * @param topic the topic
     * @param partition the partition
     * @param metricName the metric name
     * @param toIncrease the value to increase the counter by
     * @param additionalTags the additional tags
     */
    public void incrementCounter(String topic, Integer partition, String metricName, long toIncrease, String... additionalTags) {
        if (metricsConfiguration.getMetricsReporterClassName().equals(NoOpMetricsReporter.class.getName())) {
            return;
        }
        MetricRegistryAndReporter metricRegistryAndReporter = getOrCreateMetricRegistryAndReporter(topic, partition, additionalTags);
        metricRegistryAndReporter.getMetricRegistry().counter(metricName).inc(toIncrease);
    }

    /**
     * Updates counter for the given topic, partition, and metricName with the given newValue.
     * @param topic the topic
     * @param partition the partition
     * @param metricName the metric name
     * @param newValue the new value
     * @param additionalTags the additional tags
     * @return true if the counter was updated successfully, false otherwise.
     */
    public boolean updateCounter(String topic, Integer partition, String metricName, long newValue, String... additionalTags) {
        if (metricsConfiguration.getMetricsReporterClassName().equals(NoOpMetricsReporter.class.getName())) {
            return false;
        }
        MetricRegistryAndReporter metricRegistryAndReporter = getOrCreateMetricRegistryAndReporter(topic, partition, additionalTags);
        long currCount = metricRegistryAndReporter.getMetricRegistry().counter(metricName).getCount();
        metricRegistryAndReporter.getMetricRegistry().counter(metricName).dec(currCount);
        metricRegistryAndReporter.getMetricRegistry().counter(metricName).inc(newValue);
        return true;
    }

    /**
     * Updates counter for the given topic, partition, and metricName with the given newValue and reports the metric immediately.
     * This is useful for reporting metrics upon an error or exception that may cause the application to exit.
     * @param topic the topic
     * @param partition the partition
     * @param metricName the metric name
     * @param newValue the new value of the counter
     * @param additionalTags the additional tags
     */
    public void updateCounterAndReport(String topic, Integer partition, String metricName, long newValue, String... additionalTags) {
        if (!updateCounter(topic, partition, metricName, newValue, additionalTags))
            return;
        getOrCreateMetricRegistryAndReporter(topic, partition, additionalTags).getReporter().report();
    }

    /**
     * Returns the MetricsReporter instance for the given MetricRegistry, serializedMetricTagString, and MetricFilter.
     * @param registry
     * @param serializedMetricTagString
     * @param metricFilter
     * @return MetricsReporter
     */
    private MetricsReporter getMetricsReporter(MetricRegistry registry, String serializedMetricTagString, MetricFilter metricFilter) {
        try {
            Constructor<? extends MetricsReporter> reporterConstructor = Class.forName(metricsConfiguration.getMetricsReporterClassName())
                    .asSubclass(MetricsReporter.class).getConstructor(MetricsConfiguration.class, MetricRegistry.class, String.class, MetricFilter.class, TimeUnit.class, TimeUnit.class, String.class);
            return reporterConstructor.newInstance(metricsConfiguration, registry, serializedMetricTagString + "_reporter", metricFilter, TimeUnit.SECONDS, TimeUnit.SECONDS, serializedMetricTagString);
        } catch (NoSuchMethodException | ClassNotFoundException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            throw new UnsupportedOperationException("MetricsReporter class cannot be instantiated: " + metricsConfiguration.getMetricsReporterClassName(), e);
        }
    }

    /**
     * Returns the MetricRegistryAndReporter instance for the given topic, partition, and additionalTags.
     * @param topic the topic
     * @param partition the partition
     * @param additionalTags the additional tags
     * @return MetricRegistryAndReporter
     */
    private MetricRegistryAndReporter getOrCreateMetricRegistryAndReporter(String topic, Integer partition, String... additionalTags) {
        String serializedMetricTagString = getSerializedMetricTagString(topic, partition, additionalTags);
        return metricRegistryAndReporterMap.computeIfAbsent(serializedMetricTagString, k -> {
            MetricRegistry registry = new MetricRegistry();
            ScheduledReporter reporter = getMetricsReporter(registry, serializedMetricTagString, (s, m) -> true);
            executorService.scheduleAtFixedRate(
                    reporter::report,
                    Math.abs(serializedMetricTagString.hashCode()) % REPORTER_FREQUENCY_MS,
                    REPORTER_FREQUENCY_MS,
                    TimeUnit.MILLISECONDS
            );
            return new MetricRegistryAndReporter(registry, reporter);
        });
    }

    /**
     * Returns the serialized metric tag string for the given topic, partition, and additionalTags.
     * @param topic the topic
     * @param partition the partition
     * @param additionalTags the additional tags
     * @return the serialized metric tag string
     */
    private String getSerializedMetricTagString(String topic, Integer partition, String... additionalTags) {
        StringBuilder sb = new StringBuilder();
        if (topic != null) {
            sb.append("topic=").append(topic).append(" ");
        }
        if (partition != null) {
            sb.append("partition=").append(partition).append(" ");
        }
        if (additionalTags != null && additionalTags.length > 0) {
            Arrays.asList(additionalTags).forEach(t -> {
                if (t.split("=").length != 2)
                    LOG.warn(String.format("Invalid tag; requires format k=v: %s", t));
                else
                    sb.append(t).append(" ");
            });
        }
        // remove last space
        return sb.delete(sb.length() - 1, sb.length()).toString();
    }

    /**
     * Returns the counter for the given metricName, topic, partition, and additionalTags.
     * @param metricName the metric name
     * @param topic the topic
     * @param partition the partition
     * @param additionalTags the additional tags
     * @return the long value of the counter
     */
    public long getCounter(String metricName, String topic, int partition, String... additionalTags) {
        MetricRegistry metricRegistry = getMetricRegistry(topic, partition, additionalTags);
        if (metricRegistry == null) return -1L;
        return metricRegistry.counter(metricName) == null ? -1L : metricRegistry.counter(metricName).getCount();
    }

    /**
     * Returns the histogram for the given metricName, topic, partition, and additionalTags.
     * @param metricName the metric name
     * @param topic the topic
     * @param partition the partition
     * @param additionalTags the additional tags
     * @return Snapshot
     */
    public Snapshot getHistogram(String metricName, String topic, int partition, String... additionalTags) {
        MetricRegistry metricRegistry = getMetricRegistry(topic, partition, additionalTags);
        if (metricRegistry == null) return null;
        return metricRegistry.histogram(metricName) == null ? null : metricRegistry.histogram(metricName).getSnapshot();
    }

    /**
     * Returns the MetricRegistry for the given topic, partition, and additionalTags.
     * @param topic the topic
     * @param partition the partition
     * @param additionalTags the additional tags
     * @return MetricRegistry
     */
    private MetricRegistry getMetricRegistry(String topic, int partition, String[] additionalTags) {
        String serializedMetricTagString = getSerializedMetricTagString(topic, partition, additionalTags);
        MetricRegistryAndReporter metricRegistryAndReporter = metricRegistryAndReporterMap.get(serializedMetricTagString);
        if (metricRegistryAndReporter == null) {
            return null;
        }
        return metricRegistryAndReporter.getMetricRegistry();
    }

    /**
     * Returns the singleton ThreadLocal MetricRegistryManager instance with the given MetricsConfiguration.
     * @param metricsConfiguration
     * @return MetricRegistryManager
     */
    public static MetricRegistryManager getInstance(MetricsConfiguration metricsConfiguration) {
        if (metricRegistryManager.get() == null) {
            LOG.info("Creating ThreadLocal<MetricRegistryManager> for thread=" + Thread.currentThread().getName() + " with new MetricRegistryManager instance. MetricsConfiguration=" + metricsConfiguration);
            metricRegistryManager.set(new MetricRegistryManager(metricsConfiguration));
        }
        return metricRegistryManager.get();
    }

    @VisibleForTesting
    protected static int getRefCount() {
        return refCount.get();
    }
}
