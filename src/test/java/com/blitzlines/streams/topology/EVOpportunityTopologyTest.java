package com.blitzlines.streams.topology;

import com.blitzlines.streams.model.JoinedOdds;
import com.blitzlines.streams.model.TransformedLine;
import com.blitzlines.streams.serde.StreamSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EVOpportunityTopology using TopologyTestDriver.
 * Tests the KTable join logic without a running Kafka cluster.
 */
class EVOpportunityTopologyTest {
    
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, TransformedLine> pinnacleInput;
    private TestInputTopic<String, TransformedLine> kalshiInput;
    private TestInputTopic<String, TransformedLine> bovadaInput;
    private TestOutputTopic<String, JoinedOdds> joinedOutput;
    
    private final Serde<String> stringSerde = StreamSerdes.stringSerde();
    private final Serde<TransformedLine> lineSerde = StreamSerdes.transformedLineSerde();
    private final Serde<JoinedOdds> joinedOddsSerde = StreamSerdes.joinedOddsSerde();

    @BeforeEach
    void setup() {
        // Build the topology
        EVOpportunityTopology topologyBuilder = new EVOpportunityTopology();
        Topology topology = topologyBuilder.build();
        
        // Configure test driver
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-odds-join");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        
        testDriver = new TopologyTestDriver(topology, props);
        
        // Create test input topics
        pinnacleInput = testDriver.createInputTopic(
            EVOpportunityTopology.PINNACLE_TOPIC,
            stringSerde.serializer(),
            lineSerde.serializer()
        );
        
        kalshiInput = testDriver.createInputTopic(
            EVOpportunityTopology.KALSHI_TOPIC,
            stringSerde.serializer(),
            lineSerde.serializer()
        );
        
        bovadaInput = testDriver.createInputTopic(
            EVOpportunityTopology.BOVADA_TOPIC,
            stringSerde.serializer(),
            lineSerde.serializer()
        );
        
        // Create test output topic
        joinedOutput = testDriver.createOutputTopic(
            EVOpportunityTopology.JOINED_ODDS_TOPIC,
            stringSerde.deserializer(),
            joinedOddsSerde.deserializer()
        );
    }

    @AfterEach
    void teardown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testBasicJoin_Kalshi() {
        // Given: Pinnacle has odds of 2.00
        TransformedLine pinnacleLine = createLine("pinnacle", "game-123", "moneyline", "Eagles", 0.0, 2.00);
        
        // And: Kalshi offers 2.10 odds on the same selection
        TransformedLine kalshiLine = createLine("kalshi", "game-123", "moneyline", "Eagles", 0.0, 2.10);
        
        // When: Both lines are published
        pinnacleInput.pipeInput("key1", pinnacleLine);
        kalshiInput.pipeInput("key1", kalshiLine);
        
        // Then: A joined odds record should be emitted
        assertFalse(joinedOutput.isEmpty(), "Expected joined odds to be emitted");
        
        KeyValue<String, JoinedOdds> result = joinedOutput.readKeyValue();
        JoinedOdds joined = result.value;
        
        assertEquals("pinnacle", joined.getSharpBook());
        assertEquals("kalshi", joined.getSoftBook());
        assertEquals(2.00, joined.getSharpOdds(), 0.001);
        assertEquals(2.10, joined.getSoftOdds(), 0.001);
        assertEquals("Eagles", joined.getSelection());
        assertEquals("game-123", joined.getGameId());
    }

    @Test
    void testBasicJoin_Bovada() {
        // Given: Pinnacle has odds of 1.91
        TransformedLine pinnacleLine = createLine("pinnacle", "game-456", "spread", "Cowboys -3.5", -3.5, 1.91);
        
        // And: Bovada offers 2.05 odds
        TransformedLine bovadaLine = createLine("bovada", "game-456", "spread", "Cowboys -3.5", -3.5, 2.05);
        
        // When: Both lines are published
        pinnacleInput.pipeInput("key1", pinnacleLine);
        bovadaInput.pipeInput("key1", bovadaLine);
        
        // Then: A joined odds record should be emitted
        assertFalse(joinedOutput.isEmpty());
        
        KeyValue<String, JoinedOdds> result = joinedOutput.readKeyValue();
        JoinedOdds joined = result.value;
        
        assertEquals("pinnacle", joined.getSharpBook());
        assertEquals("bovada", joined.getSoftBook());
        assertEquals(1.91, joined.getSharpOdds(), 0.001);
        assertEquals(2.05, joined.getSoftOdds(), 0.001);
    }

    @Test
    void testNoJoinWithoutSharpBook() {
        // Given: Only soft book data (no Pinnacle)
        TransformedLine kalshiLine = createLine("kalshi", "game-789", "moneyline", "Patriots", 0.0, 1.85);
        
        // When: Only soft book line is published
        kalshiInput.pipeInput("key1", kalshiLine);
        
        // Then: No output (need both sides for join)
        assertTrue(joinedOutput.isEmpty(), "No output without sharp book data");
    }

    @Test
    void testJoinKeyMatching_DifferentSelections() {
        // Given: Two lines with same game but different selections should NOT join
        TransformedLine pinnacleLine = createLine("pinnacle", "game-abc", "moneyline", "Eagles", 0.0, 1.95);
        TransformedLine kalshiLine = createLine("kalshi", "game-abc", "moneyline", "Cowboys", 0.0, 2.10);
        
        // When: Both lines are published
        pinnacleInput.pipeInput("key1", pinnacleLine);
        kalshiInput.pipeInput("key2", kalshiLine);
        
        // Then: No join (different selections)
        assertTrue(joinedOutput.isEmpty(), "Different selections should not produce a join");
    }

    @Test
    void testJoinKeyMatching_DifferentLineValues() {
        // Given: Same game, same team, but different spread values should NOT join
        TransformedLine pinnacleLine = createLine("pinnacle", "game-spread", "spread", "Eagles -3.5", -3.5, 1.91);
        TransformedLine kalshiLine = createLine("kalshi", "game-spread", "spread", "Eagles -7.5", -7.5, 1.91);
        
        // When: Both lines are published
        pinnacleInput.pipeInput("key1", pinnacleLine);
        kalshiInput.pipeInput("key2", kalshiLine);
        
        // Then: No join (different line values in join key)
        assertTrue(joinedOutput.isEmpty(), "Different line values should not join");
    }

    @Test
    void testMultipleSoftBooks() {
        // Given: Pinnacle line
        TransformedLine pinnacleLine = createLine("pinnacle", "game-multi", "total", "Over 45.5", 45.5, 1.95);
        
        // And: Both Kalshi and Bovada have lines
        TransformedLine kalshiLine = createLine("kalshi", "game-multi", "total", "Over 45.5", 45.5, 2.10);
        TransformedLine bovadaLine = createLine("bovada", "game-multi", "total", "Over 45.5", 45.5, 2.05);
        
        // When: All three lines are published
        pinnacleInput.pipeInput("key1", pinnacleLine);
        kalshiInput.pipeInput("key1", kalshiLine);
        bovadaInput.pipeInput("key1", bovadaLine);
        
        // Then: Two joined records should be produced (one for each soft book)
        assertFalse(joinedOutput.isEmpty());
        
        int joinCount = 0;
        boolean hasKalshi = false;
        boolean hasBovada = false;
        
        while (!joinedOutput.isEmpty()) {
            JoinedOdds joined = joinedOutput.readValue();
            assertEquals("pinnacle", joined.getSharpBook());
            if ("kalshi".equals(joined.getSoftBook())) hasKalshi = true;
            if ("bovada".equals(joined.getSoftBook())) hasBovada = true;
            joinCount++;
        }
        
        assertTrue(joinCount >= 2, "Expected at least 2 joins (kalshi + bovada)");
        assertTrue(hasKalshi, "Expected kalshi join");
        assertTrue(hasBovada, "Expected bovada join");
    }

    @Test
    void testOddsUpdateTriggersNewJoin() {
        // Given: Initial join
        TransformedLine pinnacleLine = createLine("pinnacle", "game-update", "moneyline", "Ravens", 0.0, 2.00);
        TransformedLine kalshiLine = createLine("kalshi", "game-update", "moneyline", "Ravens", 0.0, 2.10);
        
        pinnacleInput.pipeInput("key1", pinnacleLine);
        kalshiInput.pipeInput("key1", kalshiLine);
        
        // Consume first join
        assertFalse(joinedOutput.isEmpty());
        JoinedOdds first = joinedOutput.readValue();
        assertEquals(2.10, first.getSoftOdds(), 0.001);
        
        // When: Kalshi updates their odds
        TransformedLine kalshiUpdated = createLine("kalshi", "game-update", "moneyline", "Ravens", 0.0, 2.25);
        kalshiInput.pipeInput("key1", kalshiUpdated);
        
        // Then: New join with updated odds
        assertFalse(joinedOutput.isEmpty());
        JoinedOdds updated = joinedOutput.readValue();
        assertEquals(2.25, updated.getSoftOdds(), 0.001);
    }

    /**
     * Helper to create a TransformedLine for testing.
     * Generates realistic devigged fair odds from raw odds (assumes ~3% vig for testing).
     */
    private TransformedLine createLine(String sportsbook, String gameId, String marketType,
                                       String selection, double lineValue, double odds) {
        // For testing, assume the fair odds are slightly better than raw odds
        // This simulates a ~3% vig removal (multiply raw odds by ~1.015)
        double fairOdds = odds * 1.015;
        double fairProb = 1.0 / fairOdds;
        String devigMethod = sportsbook.equals("pinnacle") ? "power" : "mult";
        
        return new TransformedLine(
            System.currentTimeMillis(),
            sportsbook,
            gameId,
            "football",
            marketType,
            "TeamA",
            "TeamB",
            selection,
            lineValue,
            odds,
            fairOdds,
            fairProb,
            devigMethod,
            "test"
        );
    }
}
