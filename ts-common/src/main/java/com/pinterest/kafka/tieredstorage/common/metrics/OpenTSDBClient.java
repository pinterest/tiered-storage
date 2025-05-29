package com.pinterest.kafka.tieredstorage.common.metrics;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * OpenTSDBClient is a client that sends metrics to OpenTSDB server via TCP. This client is thread-safe and accessed
 * via a singleton instance.
 */
public class OpenTSDBClient {

    private static final Logger LOG = LogManager.getLogger(OpenTSDBClient.class.getName());
    private static OpenTSDBClient openTSDBClient;
    private Socket socket;
    private final InetSocketAddress address;
    private final AtomicBoolean isHealthy = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private OpenTSDBClient(MetricsConfiguration metricsConfiguration) {
        this.address = new InetSocketAddress(metricsConfiguration.getHost(), metricsConfiguration.getPort());
        this.scheduler.scheduleAtFixedRate(this::checkSocketHealth, 0, 10, TimeUnit.SECONDS);
    }

    /**
     * Close the OpenTSDBClient
     */
    public void close() {
        if (!scheduler.isShutdown())
           scheduler.shutdown();
    }

    /**
     * Get the singleton instance of OpenTSDBClient
     * @param metricsConfiguration the configuration for the OpenTSDBClient
     * @return the singleton instance of OpenTSDBClient
     */
    public static OpenTSDBClient getInstance(MetricsConfiguration metricsConfiguration) {
        if (openTSDBClient == null) {
            synchronized (OpenTSDBClient.class) {
                if (openTSDBClient == null)
                    openTSDBClient = new OpenTSDBClient(metricsConfiguration);
            }
        }
        return openTSDBClient;
    }

    /**
     * Check the health of the socket and reset it if necessary
     */
    private void checkSocketHealth() {
        if (socket == null || !socket.isConnected() || socket.isClosed()) {
            LOG.info("Socket is null, is not connected, or is closed. Creating new socket");
            socket = new Socket();

            try {
                socket.connect(address, 5000);
                socket.setKeepAlive(true);
                isHealthy.set(true);
            } catch (IOException e) {
                LOG.error(String.format("Could not connect to address %s for sending metrics. %s", address, e));
                isHealthy.set(false);
            }
        }
        if (socket.isConnected() && !socket.isClosed())
            LOG.debug(String.format("Socket is connected to %s", address));
        else {
            LOG.error("Socket is not connected after reset");
        }
    }

    /**
     * Reset the socket if it is not healthy on best-effort basis
     */
    private void resetSocket() {
        if (!socket.isClosed()) {
            try {
                socket.getInputStream().close();
                socket.getOutputStream().close();
            } catch (IOException e) {
                LOG.warn("Best-effort socket I/O streams close() failed. Will proceed with socket close()", e);
            }
            try {
                LOG.info("Closing socket");
                socket.close();
            } catch (IOException e) {
                LOG.warn("Best-effort socket close() failed. Will proceed with socket reset", e);
            }
        }
        LOG.info("Resetting socket");
        checkSocketHealth();
    }

    /**
     * Send a Double metric to OpenTSDB server
     * @param name the name of the metric
     * @param value the value of the metric
     * @param serializedTagString the serialized tags for the metric as a string
     */
    public void sendMetric(String name, Double value, String serializedTagString) {

        if (!isHealthy.get()) {
            LOG.error("MetricPusher is not healthy (likely at application startup or it cannot push metrics to the designated address)");
            return;
        }

        String str = String.format("put %s %d %f %s%n",
                name, System.currentTimeMillis(), value, serializedTagString);

        writeToSocket(str);
    }

    /**
     * Send a Long metric to OpenTSDB server
     * @param name the name of the metric
     * @param value the value of the metric
     * @param serializedTagString the serialized tags for the metric as a string
     */
    public void sendMetric(String name, Long value, String serializedTagString) {

        if (!isHealthy.get()) {
            LOG.error("MetricPusher is not healthy (likely at application startup or it cannot push metrics to the designated address)");
            return;
        }

        String str = String.format("put %s %d %d %s%n",
                name, System.currentTimeMillis(), value, serializedTagString);

        writeToSocket(str);
    }

    /**
     * Executes writing to the OpenTSDB socket
     * @param putString the string to write to the socket
     */
    private void writeToSocket(String putString) {
        // try up to 5 times to send a metric
        for (int i = 1; i <= 5; ++i) {
            try {
                socket.getOutputStream().write(putString.getBytes(StandardCharsets.UTF_8));
                break;
            } catch (SocketException se) {
                LOG.error(String.format("Failed to send metrics, try #%d", i), se);
                resetSocket();
            } catch (IOException e) {
                LOG.error(String.format("Failed to send metrics, try #%d", i), e);
            }
        }
    }
}
