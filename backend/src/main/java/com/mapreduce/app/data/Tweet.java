package com.mapreduce.app.data;

import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class Tweet {
    private String message;
    private String hashtag;
}
