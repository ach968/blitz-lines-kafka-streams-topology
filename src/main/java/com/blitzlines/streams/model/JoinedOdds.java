package com.blitzlines.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * JoinedOdds - Output from Kafka Streams KTable join.
 * Contains matched odds from sharp book and soft book for the same bet.
 * 
 * Includes SIMD-devigged fair odds from sharp book for accurate EV calculation.
 * The C++ SIMD engine downstream will calculate EV% and Kelly criterion using:
 *   EV = (soft_odds × sharp_fair_prob) - 1
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class JoinedOdds {
    
    @JsonProperty("time")
    private long timestamp;           // When the join occurred (ms since epoch)
    
    @JsonProperty("game_id")
    private String gameId;            // Canonical event ID
    
    @JsonProperty("event_time")
    private long eventTime;           // Game start time (epoch ms) - for uniqueness
    
    @JsonProperty("sport")
    private String sport;             // Sport identifier
    
    @JsonProperty("market_type")
    private String marketType;        // "moneyline", "spread", "total"
    
    @JsonProperty("home_team")
    private String homeTeam;          // Home team
    
    @JsonProperty("away_team")
    private String awayTeam;          // Away team
    
    @JsonProperty("selection")
    private String selection;         // The specific bet selection
    
    @JsonProperty("line_value")
    private double lineValue;         // Line value
    
    @JsonProperty("sharp_book")
    private String sharpBook;         // Sharp book name (e.g., "pinnacle")
    
    @JsonProperty("soft_book")
    private String softBook;          // Soft book name (e.g., "kalshi", "bovada")
    
    @JsonProperty("sharp_odds")
    private double sharpOdds;         // Sharp book raw decimal odds
    
    @JsonProperty("sharp_fair_odds")
    private double sharpFairOdds;     // Sharp book DEVIGGED fair odds (no vig)
    
    @JsonProperty("sharp_fair_prob")
    private double sharpFairProb;     // Sharp book fair probability (1/fairOdds)
    
    @JsonProperty("soft_odds")
    private double softOdds;          // Soft book decimal odds (what we bet at)

    // Default constructor for Jackson
    public JoinedOdds() {}

    // Constructor from two TransformedLine records
    public JoinedOdds(TransformedLine sharpLine, TransformedLine softLine) {
        this.timestamp = System.currentTimeMillis();
        this.gameId = sharpLine.getGameId();
        this.eventTime = sharpLine.getEventTime();
        this.sport = sharpLine.getSport();
        this.marketType = sharpLine.getMarketType();
        this.homeTeam = sharpLine.getHomeTeam();
        this.awayTeam = sharpLine.getAwayTeam();
        this.selection = sharpLine.getSelection();
        this.lineValue = sharpLine.getLineValue();
        this.sharpBook = sharpLine.getSportsbook();
        this.softBook = softLine.getSportsbook();
        this.sharpOdds = sharpLine.getOdds();
        this.sharpFairOdds = sharpLine.getFairOdds();
        this.sharpFairProb = sharpLine.getFairProb();
        this.softOdds = softLine.getOdds();
    }

    // Getters and Setters
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public String getGameId() { return gameId; }
    public void setGameId(String gameId) { this.gameId = gameId; }

    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }

    public String getSport() { return sport; }
    public void setSport(String sport) { this.sport = sport; }

    public String getMarketType() { return marketType; }
    public void setMarketType(String marketType) { this.marketType = marketType; }

    public String getHomeTeam() { return homeTeam; }
    public void setHomeTeam(String homeTeam) { this.homeTeam = homeTeam; }

    public String getAwayTeam() { return awayTeam; }
    public void setAwayTeam(String awayTeam) { this.awayTeam = awayTeam; }

    public String getSelection() { return selection; }
    public void setSelection(String selection) { this.selection = selection; }

    public double getLineValue() { return lineValue; }
    public void setLineValue(double lineValue) { this.lineValue = lineValue; }

    public String getSharpBook() { return sharpBook; }
    public void setSharpBook(String sharpBook) { this.sharpBook = sharpBook; }

    public String getSoftBook() { return softBook; }
    public void setSoftBook(String softBook) { this.softBook = softBook; }

    public double getSharpOdds() { return sharpOdds; }
    public void setSharpOdds(double sharpOdds) { this.sharpOdds = sharpOdds; }

    public double getSharpFairOdds() { return sharpFairOdds; }
    public void setSharpFairOdds(double sharpFairOdds) { this.sharpFairOdds = sharpFairOdds; }

    public double getSharpFairProb() { return sharpFairProb; }
    public void setSharpFairProb(double sharpFairProb) { this.sharpFairProb = sharpFairProb; }

    public double getSoftOdds() { return softOdds; }
    public void setSoftOdds(double softOdds) { this.softOdds = softOdds; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinedOdds that = (JoinedOdds) o;
        return timestamp == that.timestamp &&
               eventTime == that.eventTime &&
               Double.compare(that.lineValue, lineValue) == 0 &&
               Double.compare(that.sharpOdds, sharpOdds) == 0 &&
               Double.compare(that.sharpFairOdds, sharpFairOdds) == 0 &&
               Double.compare(that.sharpFairProb, sharpFairProb) == 0 &&
               Double.compare(that.softOdds, softOdds) == 0 &&
               Objects.equals(gameId, that.gameId) &&
               Objects.equals(sport, that.sport) &&
               Objects.equals(marketType, that.marketType) &&
               Objects.equals(homeTeam, that.homeTeam) &&
               Objects.equals(awayTeam, that.awayTeam) &&
               Objects.equals(selection, that.selection) &&
               Objects.equals(sharpBook, that.sharpBook) &&
               Objects.equals(softBook, that.softBook);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, eventTime, gameId, sport, marketType, homeTeam, awayTeam,
                           selection, lineValue, sharpBook, softBook, sharpOdds, sharpFairOdds,
                           sharpFairProb, softOdds);
    }

    @Override
    public String toString() {
        return "JoinedOdds{" +
               "timestamp=" + timestamp +
               ", eventTime=" + eventTime +
               ", gameId='" + gameId + '\'' +
               ", sport='" + sport + '\'' +
               ", marketType='" + marketType + '\'' +
               ", homeTeam='" + homeTeam + '\'' +
               ", awayTeam='" + awayTeam + '\'' +
               ", selection='" + selection + '\'' +
               ", lineValue=" + lineValue +
               ", sharpBook='" + sharpBook + '\'' +
               ", softBook='" + softBook + '\'' +
               ", sharpOdds=" + sharpOdds +
               ", sharpFairOdds=" + sharpFairOdds +
               ", sharpFairProb=" + sharpFairProb +
               ", softOdds=" + softOdds +
               '}';
    }
}
