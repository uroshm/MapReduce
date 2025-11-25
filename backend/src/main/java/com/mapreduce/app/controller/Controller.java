package com.mapreduce.app.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {

    @PostMapping("/process")
    public String processTweets() {
        return "Working...";
    }

}
