package com.blitzlines.streams;

import com.blitzlines.streams.topology.EVOpportunityTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Main entry point for the EV Opportunity Kafka Streams application.
 * 
 * This application detects +EV (positive expected value) betting opportunities
 * by comparing odds from sharp books (Pinnacle) against soft books (Kalshi, Bovada).
 * 
 * Configuration is read from environment variables:
 * - KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9092)
 * - APPLICATION_ID: Streams application ID (default: ev-opportunity-streams)
 * - STATE_DIR: Directory for RocksDB state stores (default: /tmp/kafka-streams)
 * - COMMIT_INTERVAL_MS: Commit interval in milliseconds (default: 500)
 * - CACHE_MAX_BYTES: Max cache bytes for buffering (default: 10485760 = 10MB)
 */
public class EVOpportunityStreamsApp {
    
    private static final Logger log = LoggerFactory.getLogger(EVOpportunityStreamsApp.class);
    
    // Configuration defaults
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_APPLICATION_ID = "ev-opportunity-streams";
    private static final String DEFAULT_STATE_DIR = "/tmp/kafka-streams";
    private static final String DEFAULT_COMMIT_INTERVAL_MS = "500";
    private static final String DEFAULT_CACHE_MAX_BYTES = "10485760"; // 10MB

    public static void main(String[] args) {
        log.info("Starting EV Opportunity Streams Application...");
        
        // Build configuration from environment
        Properties props = buildConfig();
        logConfiguration(props);
        
        // Build the topology
        EVOpportunityTopology topologyBuilder = new EVOpportunityTopology();
        Topology topology = topologyBuilder.build();
        
        // Create and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(topology, props);
        
        // Setup shutdown hook for graceful termination
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                log.info("Shutdown signal received, closing streams...");
                streams.close(Duration.ofSeconds(30));
                latch.countDown();
            }
        });
        
        // Set state listener for monitoring
        streams.setStateListener((newState, oldState) -> {
            log.info("State transition: {} -> {}", oldState, newState);
            if (newState == KafkaStreams.State.ERROR) {
                log.error("Streams entered ERROR state, shutting down...");
                streams.close();
                latch.countDown();
            }
        });
        
        // Set uncaught exception handler
        streams.setUncaughtExceptionHandler((exception) -> {
            log.error("Uncaught exception in stream thread", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        
        try {
            // Start the streams application
            streams.start();
            log.info("Streams application started successfully. Listening for odds updates...");
            
            // Wait for shutdown signal
            latch.await();
        } catch (InterruptedException e) {
            log.warn("Application interrupted", e);
            Thread.currentThread().interrupt();
        } finally {
            streams.close();
            log.info("Streams application shut down.");
        }
    }

    /**
     * Build Kafka Streams configuration from environment variables.
     */
    private static Properties buildConfig() {
        Properties props = new Properties();
        
        // Required settings
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, 
            getEnv("APPLICATION_ID", DEFAULT_APPLICATION_ID));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 
            getEnv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS));
        
        // State store directory (RocksDB)
        props.put(StreamsConfig.STATE_DIR_CONFIG, 
            getEnv("STATE_DIR", DEFAULT_STATE_DIR));
        
        // Performance tuning
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 
            getEnv("COMMIT_INTERVAL_MS", DEFAULT_COMMIT_INTERVAL_MS));
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 
            getEnv("CACHE_MAX_BYTES", DEFAULT_CACHE_MAX_BYTES));
        
        // Processing guarantees
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, 
            StreamsConfig.AT_LEAST_ONCE);
        
        // Consumer settings for low latency
        props.put(StreamsConfig.consumerPrefix("max.poll.records"), "500");
        props.put(StreamsConfig.consumerPrefix("fetch.min.bytes"), "1");
        props.put(StreamsConfig.consumerPrefix("fetch.max.wait.ms"), "100");
        
        // Producer settings for batching
        props.put(StreamsConfig.producerPrefix("linger.ms"), "5");
        props.put(StreamsConfig.producerPrefix("batch.size"), "16384");
        
        // Replication factor for internal topics (set to 1 for single-node dev)
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 
            Integer.parseInt(getEnv("REPLICATION_FACTOR", "1")));
        
        // Number of stream threads (match to available cores)
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 
            Integer.parseInt(getEnv("NUM_STREAM_THREADS", "2")));
        
        return props;
    }

    /**
     * Get environment variable with fallback to default value.
     */
    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }

    /**
     * Log the current configuration.
     */
    private static void logConfiguration(Properties props) {
        log.info("=== Kafka Streams Configuration ===");
        log.info("Application ID: {}", props.get(StreamsConfig.APPLICATION_ID_CONFIG));
        log.info("Bootstrap Servers: {}", props.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        log.info("State Directory: {}", props.get(StreamsConfig.STATE_DIR_CONFIG));
        log.info("Commit Interval: {} ms", props.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
        log.info("Cache Max Bytes: {}", props.get(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG));
        log.info("Stream Threads: {}", props.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG));
        log.info("Replication Factor: {}", props.get(StreamsConfig.REPLICATION_FACTOR_CONFIG));
        log.info("===================================");
    }
}
