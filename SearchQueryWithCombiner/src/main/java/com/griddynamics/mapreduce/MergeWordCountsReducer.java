package com.griddynamics.mapreduce;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultiset;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static java.util.stream.Collectors.toList;

public class MergeWordCountsReducer extends
        Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMultiset<String> words = HashMultiset.create();
        int limit = context.getConfiguration().getInt("search.words.count.limit.output", 3);
        for (Text val : values) {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Integer> map = mapper.readValue(val.toString(), Map.class);
            map.entrySet().forEach(t -> words.add(t.getKey(), t.getValue()));
        }
        PriorityQueue<SearchWordCount> priorityQueue = new PriorityQueue(
                Comparator.comparingLong(a -> -((SearchWordCount) a).getCount()));
        words.entrySet().forEach(t -> priorityQueue.add(new SearchWordCount(t.getElement(), words.count(t.getElement()))));
        List<String> popularWords = priorityQueue.stream().limit(limit).map(t -> t.getWord()).collect(toList());
        context.write(key, new Text(popularWords.toString()));
    }
}