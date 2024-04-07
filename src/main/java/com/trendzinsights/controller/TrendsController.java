package com.trendzinsights.controller;

import com.trendzinsights.model.payload.Trend;
import com.trendzinsights.repository.TrendRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class TrendsController {

    private final TrendRepository trendRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Autowired
    public TrendsController(TrendRepository trendRepository) {
        this.trendRepository = trendRepository;
    }

    @GetMapping("/trends")
    public ResponseEntity<String> getTrendsForNextHour(@RequestBody String requestBody) {
        HttpHeaders headers = new HttpHeaders();
        String hour = null;
        try {
            hour = objectMapper.readTree(requestBody).get("hour").textValue();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return ResponseEntity.badRequest().body("Invalid JSON format");
        }

        if (hour == null || hour.isEmpty()) {
            return ResponseEntity.badRequest().body("Hour parameter is missing or empty");
        }

        // Get trends for the specified hour from the repository
        List<Trend> trends = trendRepository.getAllTrendsForNextHour(hour);

        try {
            String jsonTrends = objectMapper.writeValueAsString(trends);
            return ResponseEntity.ok(jsonTrends);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error processing JSON");
        }
    }
}

