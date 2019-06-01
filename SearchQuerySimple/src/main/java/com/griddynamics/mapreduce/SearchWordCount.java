package com.griddynamics.mapreduce;

public class SearchWordCount implements Comparable {
    private final String word;
    private final long count;

    public SearchWordCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public int compareTo(Object o) {
        return Long.compare(((SearchWordCount) o).count, this.count); // reverse order
    }

    public String getWord() {
        return word;
    }
}
