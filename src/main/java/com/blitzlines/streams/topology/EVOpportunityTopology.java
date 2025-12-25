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
 * Kafka Streams topology for DEDUPLICATING and JOINING sportsbook odds in a single step.
 * 
 * This topology:
 * 1. Creates deduplicated KTables for each sportsbook (Pinnacle, Kalshi, Bovada)
 *    - Uses aggregate() to compare incoming odds against stored odds
 *    - Only updates KTable state when odds actually CHANGE
 *    - Prevents redundant join emissions when scrapers republish unchanged lines
 * 2. Re-keys records by composite join key: {eventTime}|{gameId}|{marketType}|{selection}|{lineValue}
 * 3. Joins soft book KTables against sharp book (Pinnacle) KTable
 * 4. Outputs joined odds pairs to joined-odds topic
 * 
 * The C++ SIMD engine downstream consumes from joined-odds and performs:
 * - EV% calculation using sharp book fair probability
 * - Kelly criterion sizing
 * - WebSocket publishing
 * 
 * Architecture:
 * - Pinnacle is the sharp book (fair value proxy)
 * - Kalshi and Bovada are soft books
 * - Each sportsbook maintains its own KTable keyed by composite bet identifier
 * - KTable-KTable joins ensure we always compare latest odds
 * - DEDUP + JOIN in one step eliminates need for intermediate *-canonical topics
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
     * Create a KTable for a sportsbook topic.
     * 
     * The composite key format is: {eventTime}|{gameId}|{marketType}|{selection}|{lineValue}
     * - eventTime is CRITICAL: same teams can play on different dates
     * This ensures we only join lines for the exact same bet across sportsbooks.
     * 
     * DEDUP LOGIC: Uses aggregate() to compare incoming odds against stored odds.
     * Only updates the KTable (and triggers downstream joins) when odds actually change.
     * This prevents redundant join emissions when scrapers republish unchanged lines.
     * 
     * @param topic The source topic name
     * @param storeName The state store name for this KTable
     * @return KTable keyed by composite join key (deduplicated by odds value)
     */
    private KTable<String, TransformedLine> createSportsbookTable(String topic, String storeName) {
        log.info("Creating deduplicated KTable for topic: {} with store: {}", topic, storeName);
        
        return builder
            // Consume from topic with String key and TransformedLine value
            .stream(topic, Consumed.with(stringSerde, lineSerde))
            // Re-key by composite join key
            .selectKey((originalKey, line) -> line.getJoinKey())
            // Group by the new key for aggregation
            .groupByKey(Grouped.with(stringSerde, lineSerde))
            // Aggregate with dedup: only update when odds change
            .aggregate(
                // Initializer: null means no previous value
                () -> null,
                // Aggregator: compare odds, only update if changed
                (key, newLine, currentLine) -> {
                    if (currentLine == null) {
                        // First record for this key - always accept
                        log.debug("New line: {} -> odds={}", key, newLine.getOdds());
                        return newLine;
                    }
                    
                    // Compare odds (with small epsilon for floating point)
                    if (Math.abs(currentLine.getOdds() - newLine.getOdds()) > 0.0001) {
                        // Odds changed - update and trigger downstream join
                        log.info("Odds changed: {} | {} -> {} ({})",
                            key, currentLine.getOdds(), newLine.getOdds(), newLine.getSportsbook());
                        return newLine;
                    }
                    
                    // Odds unchanged - return current to avoid triggering join
                    // Note: KTable will still "see" an update, but value is identical
                    // so downstream joins won't produce duplicate output
                    log.trace("Odds unchanged: {} @ {}", key, currentLine.getOdds());
                    return currentLine;
                },
                // Materialized state store configuration
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
