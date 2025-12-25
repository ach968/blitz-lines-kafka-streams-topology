package com.blitzlines.streams.serde;

import com.blitzlines.streams.model.JoinedOdds;
import com.blitzlines.streams.model.TransformedLine;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * Factory class for creating Kafka Serdes used in the streams topology.
 * Provides type-safe access to serializers/deserializers for domain objects.
 */
public final class StreamSerdes {
    
    private StreamSerdes() {
        // Utility class - prevent instantiation
    }

    /**
     * Serde for TransformedLine (input from sportsbook topics).
     */
    public static Serde<TransformedLine> transformedLineSerde() {
        return new JsonSerde<>(TransformedLine.class);
    }

    /**
     * Serde for JoinedOdds (output to joined-odds topic for C++ engine).
     */
    public static Serde<JoinedOdds> joinedOddsSerde() {
        return new JsonSerde<>(JoinedOdds.class);
    }

    /**
     * String Serde (for keys).
     */
    public static Serde<String> stringSerde() {
        return Serdes.String();
    }
}
