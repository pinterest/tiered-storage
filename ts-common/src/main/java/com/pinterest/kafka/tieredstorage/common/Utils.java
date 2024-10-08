package com.pinterest.kafka.tieredstorage.common;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.core.SdkSystemSetting;

import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;

public class Utils {

    private final static Logger LOG = LogManager.getLogger(Utils.class);

    public static boolean isEc2Host() {
        try {
            String hostAddressForEC2MetadataService = SdkSystemSetting.AWS_EC2_METADATA_SERVICE_ENDPOINT.getStringValueOrThrow();
            if (hostAddressForEC2MetadataService == null)
                return false;
            URL url = new URL(hostAddressForEC2MetadataService + "/latest/dynamic/instance-identity/document");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.setConnectTimeout(1000);
            con.setReadTimeout(1000);
            con.connect();
            con.disconnect();
            return con.getResponseCode() == 200;
        } catch (ConnectException connectException) {
            return isEc2HostAlternate();
        } catch (Exception exception) {
            LOG.warn("Error occurred when determining the host type.", exception);
            return false;
        }
    }

    private static boolean isEc2HostAlternate() {
        ProcessBuilder processBuilder = new ProcessBuilder("ec2metadata");
        processBuilder.redirectErrorStream(true);
        try {
            Process process = processBuilder.start();
            return process.waitFor() == 0;
        } catch (IOException | InterruptedException e) {
            LOG.info("Error occurred when running the `ec2metadata` command. Will check OS version as last resort.");
            return System.getProperty("os.version").contains("aws");
        }
    }

    /**
     * Get a binary hash for the combination of cluster, topic, and partition. This is used for evenly distributing load
     * across multiple s3 partitions. The number of s3 partitions possible is 2^numDigits. The same hashing algorithm and
     * numDigits must be used on both the uploader and consumer side.
     * @param cluster
     * @param topic
     * @param partition
     * @param numDigits
     * @return the leftmost numDigits of the binary hash of the combination of cluster, topic, and partition
     */
    public static String getBinaryHashForClusterTopicPartition(String cluster, String topic, int partition, int numDigits) {
        if (cluster.isEmpty() || topic.isEmpty()) {
            throw new IllegalArgumentException("Cluster and topic must be non-empty");
        }
        String combination = cluster + "-" + topic + "-" + partition;
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] bytes = digest.digest(combination.getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                String binaryString = String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
                sb.append(binaryString);
                if (sb.length() >= numDigits) {
                    break;
                }
            }
            return sb.substring(0, numDigits);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to get binary hash for cluster=%s, topic=%s, partition=%s", cluster, topic, partition), e);
        }
    }
}
