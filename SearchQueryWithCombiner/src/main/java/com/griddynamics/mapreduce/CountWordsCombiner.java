package com.griddynamics.mapreduce;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultiset;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CountWordsCombiner extends
        Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {

        HashMultiset<String> words =  HashMultiset.create();
        StreamSupport.stream(values.spliterator(), false).forEach(t -> words.add(t.toString()));
        Map<String, Integer> result = words.entrySet().stream().collect(Collectors.toMap(x -> x.getElement(), x -> x.getCount()));
        ObjectMapper mapper = new ObjectMapper();
        String jsonResult = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result);
        context.write(key, new Text(jsonResult));
    }
}