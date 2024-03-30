package com.example.springbootkafkanishant.repository;

import com.example.springbootkafkanishant.model.payload.Tweet;
import scala.Serializable;


public interface TweetRepository extends Serializable {
    void saveTweet(Tweet tweet);
}
