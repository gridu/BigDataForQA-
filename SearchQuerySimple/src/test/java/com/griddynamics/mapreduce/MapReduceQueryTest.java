package com.griddynamics.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MapReduceQueryTest {
    private final String[] UUIDs = {"74492f56-59cd-4759-b357-9817285cc39e", "a2ce8e4e-0bf6-4fa8-94a5-4317379a4daf"};
    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        IDToWordMapper mapper = new IDToWordMapper();
        CountWordsReducer reducer = new CountWordsReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "2019-04-23 17:13:52.155578\t" + UUIDs[0] + "\t\"Calvin Klein jeans\""));
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "2019-04-23 17:13:52.155670\t" + UUIDs[0] + "\t\"Calvin Klein dress\""));
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "2019-04-23 17:13:52.155702\t" + UUIDs[0] + "\t\"red dress\""));
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "2019-04-23 17:13:52.155740\t" + UUIDs[0] + "\t\"Klein watch\""));
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "2019-04-23 17:13:52.155798\t" + UUIDs[1] + "\t\"bla bla\""));

        mapReduceDriver.withOutput(new Text(UUIDs[0]), new Text("[Klein, dress, Calvin]"));
        mapReduceDriver.withOutput(new Text(UUIDs[1]), new Text("[bla]"));
        mapReduceDriver.runTest();
    }

}