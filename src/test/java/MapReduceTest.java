import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    private final String testSTR= "10;50;1;1647033792\n";
    private final String sectorName = "sec_0";

    @Before
    public void setUp() {
        URI[] cacheFiles = new URI[2];
        cacheFiles[0] = new Path("sectors.json").toUri();
        cacheFiles[1] = new Path("temperature.json").toUri();
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapDriver.setCacheFiles(cacheFiles);
        reduceDriver.setCacheFiles(cacheFiles);
        mapReduceDriver.setCacheFiles(cacheFiles);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testSTR))
                .withOutput(new Text(sectorName), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(7));
        values.add(new IntWritable(5));
        reduceDriver
                .withInput(new Text(sectorName), values)
                .withOutput(new Text(sectorName), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testSTR))
                .withInput(new LongWritable(), new Text(testSTR))
                .withOutput(new Text(sectorName), new IntWritable(1))
                .runTest();
    }
}