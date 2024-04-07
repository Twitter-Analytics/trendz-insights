package com.trendzinsights.repository;

import com.trendzinsights.model.payload.Trend;
import org.springframework.stereotype.Repository;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Repository
public class TrendRepositaryImplementaion implements TrendRepository{

    @Override
    public void saveTrend(Trend trend) {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/tweetsAnalysis", "nishant", "nishant")) {
            System.out.println("Pushing trends in trend!");
            String sql = "INSERT INTO trends (name, sentimentScore, positive1, positive2, negative1, negative2, hour) VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, trend.getName());
                statement.setString(2, trend.getSentimentScore());
                statement.setString(3 , trend.getPositive1());
                statement.setString(4 , trend.getPositive2());
                statement.setString(5 , trend.getNegative1());
                statement.setString(6 , trend.getNegative2());
                statement.setString(7 , trend.getHour());

                statement.executeUpdate();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Trend> getAllTrendsForNextHour(String hour) {

        List<Trend> trends = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/tweetsAnalysis", "nishant", "nishant")) {
            String sqlQuery = "SELECT * FROM trend " +
                    "WHERE to_timestamp(hour, 'YYYY-MM-DD HH24:MI:SS+TZH:TZM') >= '" + hour + "' " +
                    "AND to_timestamp(hour, 'YYYY-MM-DD HH24:MI:SS+TZH:TZM') < (TIMESTAMP '" + hour + "' + INTERVAL '1 hour')";
//            String sqlQuery = "SELECT * FROM trend";
            try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
                statement.setString(1, hour);
                statement.setString(2, hour);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Trend trend = new Trend();
                        trend.setName(resultSet.getString("name"));
                        trend.setSentimentScore(resultSet.getString("sentimentScore"));
                        trend.setPositive1(resultSet.getString("positive1"));
                        trend.setPositive2(resultSet.getString("positive2"));
                        trend.setNegative1(resultSet.getString("negative1"));
                        trend.setNegative2(resultSet.getString("negative2"));
                        trend.setHour(resultSet.getString("hour"));
                        trends.add(trend);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(trends);
        return trends;
    }

}
