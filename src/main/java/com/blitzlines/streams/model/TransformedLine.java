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
    
    @JsonProperty("scraped_at")
    private long scrapedAt;           // When captured (epoch ms) - for staleness checks
    
    @JsonProperty("sportsbook")
    private String sportsbook;        // "pinnacle", "kalshi", "bovada"
    
    @JsonProperty("game_id")
    private String gameId;            // Canonical event ID: {sport}-{league}-{date}-{away}-{home}
    
    @JsonProperty("event_time")
    private long eventTime;           // Game start time (epoch ms) - CRITICAL for join key uniqueness
    
    @JsonProperty("sport")
    private String sport;             // "football", "basketball", etc.
    
    @JsonProperty("market_type")
    private String marketType;        // "moneyline", "spread", "total"
    
    @JsonProperty("home_team")
    private String homeTeam;          // Home team name
    
    @JsonProperty("away_team")
    private String awayTeam;          // Away team name
    
    @JsonProperty("selection")
    private String selection;         // e.g., "Eagles -3.5", "Over 45.5"
    
    @JsonProperty("line_value")
    private double lineValue;         // Line value (-3.5, 45.5, 0 for ML)
    
    @JsonProperty("odds")
    private double odds;              // Raw decimal odds from sportsbook
    
    @JsonProperty("fair_odds")
    private double fairOdds;          // SIMD-devigged fair odds (no vig)
    
    @JsonProperty("fair_prob")
    private double fairProb;          // Fair implied probability (1/fairOdds)
    
    @JsonProperty("devig_method")
    private String devigMethod;       // Devig method: "power", "mult", "additive", "shin"
    
    @JsonProperty("data_source")
    private String dataSource;        // API endpoint or scraper identifier
    
    @JsonProperty("league")
    private String league;            // "NFL", "NBA", "MLB" - useful for filtering

    // Default constructor for Jackson
    public TransformedLine() {}

    // Full constructor matching C++ CanonicalLine
    public TransformedLine(long scrapedAt, String sportsbook, String gameId, long eventTime,
                          String sport, String league, String marketType, String homeTeam,
                          String awayTeam, String selection, double lineValue, double odds,
                          double fairOdds, double fairProb, String devigMethod, String dataSource) {
        this.scrapedAt = scrapedAt;
        this.sportsbook = sportsbook;
        this.gameId = gameId;
        this.eventTime = eventTime;
        this.sport = sport;
        this.league = league;
        this.marketType = marketType;
        this.homeTeam = homeTeam;
        this.awayTeam = awayTeam;
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
     * Format: {eventTime}|{gameId}|{marketType}|{selection}|{lineValue}
     * 
     * eventTime is CRITICAL for uniqueness - same teams can play multiple times.
     * This ensures we only join lines for the exact same bet across sportsbooks.
     */
    public String getJoinKey() {
        return String.format("%d|%s|%s|%s|%.2f", eventTime, gameId, marketType, selection, lineValue);
    }

    // Getters and Setters
    public long getScrapedAt() { return scrapedAt; }
    public void setScrapedAt(long scrapedAt) { this.scrapedAt = scrapedAt; }

    public String getSportsbook() { return sportsbook; }
    public void setSportsbook(String sportsbook) { this.sportsbook = sportsbook; }

    public String getGameId() { return gameId; }
    public void setGameId(String gameId) { this.gameId = gameId; }

    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }

    public String getSport() { return sport; }
    public void setSport(String sport) { this.sport = sport; }

    public String getLeague() { return league; }
    public void setLeague(String league) { this.league = league; }

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
        return scrapedAt == that.scrapedAt &&
               eventTime == that.eventTime &&
               Double.compare(that.lineValue, lineValue) == 0 &&
               Double.compare(that.odds, odds) == 0 &&
               Double.compare(that.fairOdds, fairOdds) == 0 &&
               Double.compare(that.fairProb, fairProb) == 0 &&
               Objects.equals(sportsbook, that.sportsbook) &&
               Objects.equals(gameId, that.gameId) &&
               Objects.equals(sport, that.sport) &&
               Objects.equals(league, that.league) &&
               Objects.equals(marketType, that.marketType) &&
               Objects.equals(homeTeam, that.homeTeam) &&
               Objects.equals(awayTeam, that.awayTeam) &&
               Objects.equals(selection, that.selection) &&
               Objects.equals(devigMethod, that.devigMethod) &&
               Objects.equals(dataSource, that.dataSource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scrapedAt, eventTime, sportsbook, gameId, sport, league, marketType,
                           homeTeam, awayTeam, selection, lineValue, odds, fairOdds, fairProb,
                           devigMethod, dataSource);
    }

    @Override
    public String toString() {
        return "TransformedLine{" +
               "scrapedAt=" + scrapedAt +
               ", eventTime=" + eventTime +
               ", sportsbook='" + sportsbook + '\'' +
               ", gameId='" + gameId + '\'' +
               ", sport='" + sport + '\'' +
               ", league='" + league + '\'' +
               ", marketType='" + marketType + '\'' +
               ", homeTeam='" + homeTeam + '\'' +
               ", awayTeam='" + awayTeam + '\'' +
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
