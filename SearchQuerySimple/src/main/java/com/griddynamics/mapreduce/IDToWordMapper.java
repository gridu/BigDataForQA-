package com.griddynamics.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IDToWordMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {

        for (String line : value.toString().split("\n")) {
            String[] elements = line.split("\t");
            String[] words = elements[2].replace("\"", "").split(" ");
            for (String word : words) {
                context.write(new Text(elements[1]), new Text(word));
            }
        }

    }
}