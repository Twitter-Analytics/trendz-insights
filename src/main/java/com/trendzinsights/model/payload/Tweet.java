package com.trendzinsights.model.payload;

/*
 "just column ","created_at", "tweet_id", "tweet", "likes",
 "retweet_count", "user_id", "user_followers_count", "user_location"
*/
public class Tweet {
    private String created_at , tweet_id , tweet , likes , retweet_count , user_id , user_followers_count , user_location;

    @Override
    public String toString() {
        return "Tweet{" +
                "created_at='" + created_at + '\'' +
                ", tweet_id='" + tweet_id + '\'' +
                ", tweet='" + tweet + '\'' +
                ", likes='" + likes + '\'' +
                ", retweet_count='" + retweet_count + '\'' +
                ", user_id='" + user_id + '\'' +
                ", user_followers_count='" + user_followers_count + '\'' +
                ", user_location='" + user_location + '\'' +
                '}';
    }

    public String getCreated_at() {
        return created_at;
    }

    public void setCreated_at(String created_at) {
        this.created_at = created_at;
    }

    public String getTweet_id() {
        return tweet_id;
    }

    public void setTweet_id(String tweet_id) {
        this.tweet_id = tweet_id;
    }

    public String getTweet() {
        return tweet;
    }

    public void setTweet(String tweet) {
        this.tweet = tweet;
    }

    public String getLikes() {
        return likes;
    }

    public void setLikes(String likes) {
        this.likes = likes;
    }

    public String getRetweet_count() {
        return retweet_count;
    }

    public void setRetweet_count(String retweet_count) {
        this.retweet_count = retweet_count;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getUser_followers_count() {
        return user_followers_count;
    }

    public void setUser_followers_count(String user_followers_count) {
        this.user_followers_count = user_followers_count;
    }

    public String getUser_location() {
        return user_location;
    }

    public void setUser_location(String user_location) {
        this.user_location = user_location;
    }
}
