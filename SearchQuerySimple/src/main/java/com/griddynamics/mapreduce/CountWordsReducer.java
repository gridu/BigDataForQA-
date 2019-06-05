package com.griddynamics.mapreduce;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import static java.util.stream.Collectors.toList;

public class CountWordsReducer extends
        Reducer<Text, Text, Text, Text> {


    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int limit = context.getConfiguration().getInt("search.words.count.limit.output", 3);

        Multiset<String> frequency = HashMultiset.create();
        values.forEach(t -> frequency.add(t.toString()));
        PriorityQueue<SearchWordCount> priorityQueue = new PriorityQueue(Comparator.comparingLong(a -> -((SearchWordCount) a).getCount()));
        frequency.entrySet().forEach(t -> priorityQueue.add(new SearchWordCount(t.getElement(), frequency.count(t.getElement()))));
        List<String> popularWords = priorityQueue.stream().limit(limit).map(t -> t.getWord()).collect(toList());
        context.write(key, new Text(popularWords.toString()));
    }
}