package com.blitzlines.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * TransformedLine - Unified schema for transformed Kafka topics.
 * Published by scrapers to pinny-transformed, kalshi-transformed, bovada-transformed.
 * 
 * Matches the C++ TransformedLine struct in src/models.h
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransformedLine {
    
    @JsonProperty("timestamp")
    private long timestamp;           // Unix timestamp in milliseconds
    
    @JsonProperty("sportsbook")
    private String sportsbook;        // "pinnacle", "kalshi", "bovada"
    
    @JsonProperty("gameId")
    private String gameId;            // Canonical event ID: {sport}-{league}-{date}-{away}-{home}
    
    @JsonProperty("sport")
    private String sport;             // "football", "basketball", etc.
    
    @JsonProperty("marketType")
    private String marketType;        // "moneyline", "spread", "total"
    
    @JsonProperty("teamA")
    private String teamA;             // Home team name
    
    @JsonProperty("teamB")
    private String teamB;             // Away team name
    
    @JsonProperty("selection")
    private String selection;         // e.g., "Eagles -3.5", "Over 45.5"
    
    @JsonProperty("lineValue")
    private double lineValue;         // Line value (-3.5, 45.5, 0 for ML)
    
    @JsonProperty("odds")
    private double odds;              // Raw decimal odds from sportsbook
    
    @JsonProperty("fairOdds")
    private double fairOdds;          // SIMD-devigged fair odds (no vig)
    
    @JsonProperty("fairProb")
    private double fairProb;          // Fair implied probability (1/fairOdds)
    
    @JsonProperty("devigMethod")
    private String devigMethod;       // Devig method: "power", "mult", "additive", "shin"
    
    @JsonProperty("dataSource")
    private String dataSource;        // API endpoint or scraper identifier

    // Default constructor for Jackson
    public TransformedLine() {}

    // Full constructor
    public TransformedLine(long timestamp, String sportsbook, String gameId, String sport,
                          String marketType, String teamA, String teamB, String selection,
                          double lineValue, double odds, double fairOdds, double fairProb,
                          String devigMethod, String dataSource) {
        this.timestamp = timestamp;
        this.sportsbook = sportsbook;
        this.gameId = gameId;
        this.sport = sport;
        this.marketType = marketType;
        this.teamA = teamA;
        this.teamB = teamB;
        this.selection = selection;
        this.lineValue = lineValue;
        this.odds = odds;
        this.fairOdds = fairOdds;
        this.fairProb = fairProb;
        this.devigMethod = devigMethod;
        this.dataSource = dataSource;
    }

    /**
     * Generate the join key for KTable joins.
     * Format: {gameId}|{marketType}|{selection}|{lineValue}
     * 
     * This ensures we only join lines for the exact same bet across sportsbooks.
     */
    public String getJoinKey() {
        return String.format("%s|%s|%s|%.2f", gameId, marketType, selection, lineValue);
    }

    // Getters and Setters
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public String getSportsbook() { return sportsbook; }
    public void setSportsbook(String sportsbook) { this.sportsbook = sportsbook; }

    public String getGameId() { return gameId; }
    public void setGameId(String gameId) { this.gameId = gameId; }

    public String getSport() { return sport; }
    public void setSport(String sport) { this.sport = sport; }

    public String getMarketType() { return marketType; }
    public void setMarketType(String marketType) { this.marketType = marketType; }

    public String getTeamA() { return teamA; }
    public void setTeamA(String teamA) { this.teamA = teamA; }

    public String getTeamB() { return teamB; }
    public void setTeamB(String teamB) { this.teamB = teamB; }

    public String getSelection() { return selection; }
    public void setSelection(String selection) { this.selection = selection; }

    public double getLineValue() { return lineValue; }
    public void setLineValue(double lineValue) { this.lineValue = lineValue; }

    public double getOdds() { return odds; }
    public void setOdds(double odds) { this.odds = odds; }

    public double getFairOdds() { return fairOdds; }
    public void setFairOdds(double fairOdds) { this.fairOdds = fairOdds; }

    public double getFairProb() { return fairProb; }
    public void setFairProb(double fairProb) { this.fairProb = fairProb; }

    public String getDevigMethod() { return devigMethod; }
    public void setDevigMethod(String devigMethod) { this.devigMethod = devigMethod; }

    public String getDataSource() { return dataSource; }
    public void setDataSource(String dataSource) { this.dataSource = dataSource; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransformedLine that = (TransformedLine) o;
        return timestamp == that.timestamp &&
               Double.compare(that.lineValue, lineValue) == 0 &&
               Double.compare(that.odds, odds) == 0 &&
               Double.compare(that.fairOdds, fairOdds) == 0 &&
               Double.compare(that.fairProb, fairProb) == 0 &&
               Objects.equals(sportsbook, that.sportsbook) &&
               Objects.equals(gameId, that.gameId) &&
               Objects.equals(sport, that.sport) &&
               Objects.equals(marketType, that.marketType) &&
               Objects.equals(teamA, that.teamA) &&
               Objects.equals(teamB, that.teamB) &&
               Objects.equals(selection, that.selection) &&
               Objects.equals(devigMethod, that.devigMethod) &&
               Objects.equals(dataSource, that.dataSource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, sportsbook, gameId, sport, marketType, teamA, teamB,
                           selection, lineValue, odds, fairOdds, fairProb, devigMethod, dataSource);
    }

    @Override
    public String toString() {
        return "TransformedLine{" +
               "timestamp=" + timestamp +
               ", sportsbook='" + sportsbook + '\'' +
               ", gameId='" + gameId + '\'' +
               ", sport='" + sport + '\'' +
               ", marketType='" + marketType + '\'' +
               ", teamA='" + teamA + '\'' +
               ", teamB='" + teamB + '\'' +
               ", selection='" + selection + '\'' +
               ", lineValue=" + lineValue +
               ", odds=" + odds +
               ", fairOdds=" + fairOdds +
               ", fairProb=" + fairProb +
               ", devigMethod='" + devigMethod + '\'' +
               ", dataSource='" + dataSource + '\'' +
               '}';
    }
}
