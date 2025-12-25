package com.blitzlines.streams.topology;

import com.blitzlines.streams.model.JoinedOdds;
import com.blitzlines.streams.model.TransformedLine;
import com.blitzlines.streams.serde.StreamSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams topology for joining sportsbook odds.
 * 
 * This topology:
 * 1. Creates KTables for each sportsbook (Pinnacle, Kalshi, Bovada)
 * 2. Re-keys records by composite join key: {gameId}|{marketType}|{selection}|{lineValue}
 * 3. Joins soft book KTables against sharp book (Pinnacle) KTable
 * 4. Outputs joined odds pairs to joined-odds topic
 * 
 * The C++ SIMD engine downstream consumes from joined-odds and performs:
 * - EV% calculation
 * - Kelly criterion sizing
 * - WebSocket publishing
 * 
 * Architecture:
 * - Pinnacle is the sharp book (fair value proxy)
 * - Kalshi and Bovada are soft books
 * - Each sportsbook maintains its own KTable keyed by composite bet identifier
 * - KTable-KTable joins ensure we always compare latest odds
 */
public class EVOpportunityTopology {
    
    private static final Logger log = LoggerFactory.getLogger(EVOpportunityTopology.class);
    
    // Topic names (must match scraper output topics)
    public static final String PINNACLE_TOPIC = "pinny-transformed";
    public static final String KALSHI_TOPIC = "kalshi-transformed";
    public static final String BOVADA_TOPIC = "bovada-transformed";
    public static final String JOINED_ODDS_TOPIC = "joined-odds";
    
    // State store names for KTables
    public static final String PINNACLE_STORE = "pinnacle-store";
    public static final String KALSHI_STORE = "kalshi-store";
    public static final String BOVADA_STORE = "bovada-store";
    
    private final StreamsBuilder builder;
    private final Serde<String> stringSerde;
    private final Serde<TransformedLine> lineSerde;
    private final Serde<JoinedOdds> joinedOddsSerde;

    public EVOpportunityTopology() {
        this.builder = new StreamsBuilder();
        this.stringSerde = StreamSerdes.stringSerde();
        this.lineSerde = StreamSerdes.transformedLineSerde();
        this.joinedOddsSerde = StreamSerdes.joinedOddsSerde();
    }

    /**
     * Build the complete Kafka Streams topology.
     * 
     * @return The constructed topology ready for KafkaStreams instantiation
     */
    public Topology build() {
        log.info("Building odds join topology...");
        
        // Create KTables for each sportsbook, re-keyed by composite join key
        KTable<String, TransformedLine> pinnacleTable = createSportsbookTable(PINNACLE_TOPIC, PINNACLE_STORE);
        KTable<String, TransformedLine> kalshiTable = createSportsbookTable(KALSHI_TOPIC, KALSHI_STORE);
        KTable<String, TransformedLine> bovadaTable = createSportsbookTable(BOVADA_TOPIC, BOVADA_STORE);
        
        // Join soft books against sharp book (Pinnacle)
        KStream<String, JoinedOdds> kalshiJoined = joinTables(pinnacleTable, kalshiTable, "kalshi");
        KStream<String, JoinedOdds> bovadaJoined = joinTables(pinnacleTable, bovadaTable, "bovada");
        
        // Merge all joined streams
        KStream<String, JoinedOdds> allJoined = kalshiJoined.merge(bovadaJoined);
        
        // Filter out nulls and log
        KStream<String, JoinedOdds> validJoined = allJoined
            .filter((key, joined) -> joined != null)
            .peek((key, joined) -> log.info("Joined odds: {} | {} @ {} vs {} @ {}",
                joined.getSelection(), joined.getSharpBook(), joined.getSharpOdds(),
                joined.getSoftBook(), joined.getSoftOdds()));
        
        // Output to joined-odds topic for C++ SIMD engine
        validJoined.to(JOINED_ODDS_TOPIC, Produced.with(stringSerde, joinedOddsSerde));
        
        Topology topology = builder.build();
        log.info("Topology built successfully:\n{}", topology.describe());
        
        return topology;
    }

    /**
     * Create a KTable for a sportsbook topic, re-keyed by composite join key.
     * 
     * The composite key format is: {gameId}|{marketType}|{selection}|{lineValue}
     * This ensures we only join lines for the exact same bet across sportsbooks.
     * 
     * @param topic The source topic name
     * @param storeName The state store name for this KTable
     * @return KTable keyed by composite join key
     */
    private KTable<String, TransformedLine> createSportsbookTable(String topic, String storeName) {
        log.info("Creating KTable for topic: {} with store: {}", topic, storeName);
        
        return builder
            // Consume from topic with String key and TransformedLine value
            .stream(topic, Consumed.with(stringSerde, lineSerde))
            // Re-key by composite join key
            .selectKey((originalKey, line) -> line.getJoinKey())
            // Log incoming records for debugging
            .peek((key, line) -> log.debug("Received line: {} -> odds={}", key, line.getOdds()))
            // Convert to KTable with materialized state store
            .toTable(
                Materialized.<String, TransformedLine, KeyValueStore<Bytes, byte[]>>as(storeName)
                    .withKeySerde(stringSerde)
                    .withValueSerde(lineSerde)
            );
    }

    /**
     * Join soft book KTable against sharp book KTable.
     * 
     * @param sharpTable The sharp book KTable (Pinnacle - fair value proxy)
     * @param softTable The soft book KTable (Kalshi, Bovada)
     * @param softBookName Name of the soft book for logging
     * @return Stream of JoinedOdds records
     */
    private KStream<String, JoinedOdds> joinTables(
            KTable<String, TransformedLine> sharpTable,
            KTable<String, TransformedLine> softTable,
            String softBookName) {
        
        log.info("Creating KTable join: pinnacle <-> {}", softBookName);
        
        // KTable-KTable join: triggered when either side updates
        return sharpTable.join(
            softTable,
            (sharpLine, softLine) -> {
                log.debug("Join match: {} | sharp={} soft={}",
                    sharpLine.getJoinKey(), sharpLine.getOdds(), softLine.getOdds());
                
                // Create JoinedOdds record - EV calculation happens in C++ engine
                return new JoinedOdds(sharpLine, softLine);
            },
            Materialized.with(stringSerde, joinedOddsSerde)
        )
        // Convert KTable to KStream for downstream processing
        .toStream();
    }

    /**
     * Get the underlying StreamsBuilder for testing purposes.
     */
    public StreamsBuilder getBuilder() {
        return builder;
    }
}
