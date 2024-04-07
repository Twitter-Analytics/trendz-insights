package com.trendzinsights.repository;

import com.trendzinsights.model.payload.Trend;
import scala.Serializable;

import java.util.List;

public interface TrendRepository extends Serializable {
    void saveTrend(Trend trend);
    List<Trend> getAllTrendsForNextHour(String hour);

}
