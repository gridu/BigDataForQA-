package com.griddynamics.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class IDToWordMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Context context) {
        String[] elements = value.toString().split("\t");
        String[] words = elements[2].replace("\"", "").split(" ");
        Arrays.stream(words).forEach(w -> {
            try {
                context.write(new Text(elements[1]), new Text(w));
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }
}