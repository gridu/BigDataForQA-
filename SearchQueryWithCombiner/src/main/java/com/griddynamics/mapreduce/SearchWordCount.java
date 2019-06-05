package com.griddynamics.mapreduce;

public class SearchWordCount {
    private final String word;
    private final long count;

    public SearchWordCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public long getCount() {
        return count;
    }
}
