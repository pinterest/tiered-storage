package com.pinterest.kafka.tieredstorage.common.metrics;

import java.util.Properties;

/**
 * MetricsConfiguration is a class that contains the configuration for metrics collection and reporting
 */
public class MetricsConfiguration {

    public static final String METRICS_REPORTER_CLASS_CONFIG = "metrics.reporter.class";
    public static final String METRICS_REPORTER_HOST_CONFIG = "metrics.reporter.host";
    public static final String METRICS_REPORTER_PORT_CONFIG = "metrics.reporter.port";
    public static final String METRICS_REGISTRY_MANAGER_THREAD_LOCAL_CONFIG = "metrics.registry.manager.threadLocal";
    private final boolean metricRegistryManagerThreadLocalEnabled;
    private final String metricsReporterClassName;
    private final String host;
    private final Integer port;

    public MetricsConfiguration(boolean metricRegistryManagerThreadLocalEnabled,
                                String metricsReporterClassName,
                                String host,
                                Integer port) {
        this.metricRegistryManagerThreadLocalEnabled = metricRegistryManagerThreadLocalEnabled;
        this.metricsReporterClassName = metricsReporterClassName;
        this.host = host;
        this.port = port;
    }

    /**
     * Get the {@link MetricsConfiguration} from the given properties. If no metrics reporter class is set, the metrics reporter will default to {@link NoOpMetricsReporter}
     * @param properties the properties to get the metrics configuration from
     * @return the metrics configuration
     */
    public static MetricsConfiguration getMetricsConfiguration(Properties properties) {
        String metricsReporterClassName = properties.containsKey(MetricsConfiguration.METRICS_REPORTER_CLASS_CONFIG) ?
                properties.getProperty(MetricsConfiguration.METRICS_REPORTER_CLASS_CONFIG) :
                NoOpMetricsReporter.class.getName();
        boolean metricRegistryManagerThreadLocalEnabled = Boolean.parseBoolean(properties.getProperty(MetricsConfiguration.METRICS_REGISTRY_MANAGER_THREAD_LOCAL_CONFIG, "false"));

        String metricsHost = null;
        Integer metricsPort = null;
        if (!metricsReporterClassName.equals(NoOpMetricsReporter.class.getName())) {
            // need host and port for reporter
            if (properties.containsKey(MetricsConfiguration.METRICS_REPORTER_HOST_CONFIG) && properties.containsKey(MetricsConfiguration.METRICS_REPORTER_PORT_CONFIG)) {
                metricsHost = properties.getProperty(MetricsConfiguration.METRICS_REPORTER_HOST_CONFIG);
                metricsPort = Integer.parseInt(properties.getProperty(MetricsConfiguration.METRICS_REPORTER_PORT_CONFIG));
            } else {
                throw new RuntimeException(String.format("%s and %s must be set for %s=%s",
                        MetricsConfiguration.METRICS_REPORTER_HOST_CONFIG, MetricsConfiguration.METRICS_REPORTER_PORT_CONFIG, MetricsConfiguration.METRICS_REPORTER_CLASS_CONFIG, metricsReporterClassName));
            }
        }
        return new MetricsConfiguration(metricRegistryManagerThreadLocalEnabled, metricsReporterClassName, metricsHost, metricsPort);
    }

    /**
     * Whether to use thread-local MetricRegistryManager
     * @return true if thread-local MetricRegistryManager is enabled, false otherwise
     */
    public boolean getMetricRegistryManagerThreadLocalEnabled() {
        return metricRegistryManagerThreadLocalEnabled;
    }

    /**
     * Get the metrics reporter class name
     * @return the metrics reporter class name
     */
    public String getMetricsReporterClassName() {
        return metricsReporterClassName;
    }

    /**
     * Get the metrics reporter host
     * @return the metrics reporter host
     */
    public String getHost() {
        return host;
    }

    /**
     * Get the metrics reporter port
     * @return the metrics reporter port
     */
    public Integer getPort() {
        return port;
    }

    @Override
    public String toString() {
        return String.format("MetricsConfiguration={metricsReporterClassName=%s, host=%s, port=%s}", metricsReporterClassName, host, port);
    }
}
