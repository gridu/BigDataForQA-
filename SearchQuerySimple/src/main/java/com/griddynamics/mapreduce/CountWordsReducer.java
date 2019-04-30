package com.griddynamics.mapreduce;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class CountWordsReducer extends
        Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
        List<String> terms = StreamSupport.stream(values.spliterator(), false).map(item -> item.toString()).collect(Collectors.toList());
        HashSet<String> stringsSet = new HashSet<>(terms);
        HashMap<String, Integer> counts = new HashMap<>();
        for (String s : stringsSet) {
            counts.put(s, Collections.frequency(terms, s));
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