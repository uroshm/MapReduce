package com.mapreduce.app.data;

import java.util.UUID;

import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class Tweet {
    private String hashtag;
    private String message;
    private String user;
    private UUID id;
}
