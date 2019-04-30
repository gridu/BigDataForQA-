package com.griddynamics.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CountWordsCombiner extends
        Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {

        List<String> terms = StreamSupport.stream(values.spliterator(), false).map(item ->
                item.toString()).collect(Collectors.toList());
        HashSet<String> stringsSet = new HashSet<>(terms);
        for (String word : stringsSet) {
            String format = String.format("%s\t%d", word, Collections.frequency(terms, word));
            context.write(key, new Text(format));
        }

    }
}