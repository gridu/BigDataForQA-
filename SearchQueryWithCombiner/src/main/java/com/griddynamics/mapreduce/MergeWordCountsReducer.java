package com.griddynamics.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class MergeWordCountsReducer extends
        Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
        HashMap<String, Integer> counts = new HashMap<>();
        for (Text val : values) {
            String word = val.toString().split("\t")[0];
            int frequency = Integer.valueOf(val.toString().split("\t")[1]);
            int newValue = counts.getOrDefault(word, 0) + frequency;
            counts.put(word, newValue);

        }
        LinkedHashMap<String, Integer> sorted = counts
                .entrySet()
                .stream()
                .sorted(java.util.Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2, LinkedHashMap::new));
        List<String> collect = sorted.entrySet().stream().limit(3).map(item -> item.getKey()).collect(toList());

        context.write(key, new Text(collect.toString()));
    }
}