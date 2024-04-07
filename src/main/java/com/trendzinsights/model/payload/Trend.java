package com.trendzinsights.model.payload;

public class Trend {
    private String name;
    private String hour;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getPositive1() {
        return positive1;
    }

    public void setPositive1(String positive1) {
        this.positive1 = positive1;
    }

    public String getPositive2() {
        return positive2;
    }

    public void setPositive2(String positive2) {
        this.positive2 = positive2;
    }

    public String getNegative1() {
        return negative1;
    }

    public void setNegative1(String negative1) {
        this.negative1 = negative1;
    }

    public String getNegative2() {
        return negative2;
    }

    public void setNegative2(String negative2) {
        this.negative2 = negative2;
    }

    public String getSentimentScore() {
        return sentimentScore;
    }

    public void setSentimentScore(String sentimentScore) {
        this.sentimentScore = sentimentScore;
    }

    private String positive1;
    private String positive2;
    private String negative1;
    private String negative2;
    private String sentimentScore;
}