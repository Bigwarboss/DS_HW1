import bdtc.lab1.HW1Reducer;
import eu.bitwalker.useragentutils.UserAgent;
import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;


    private final String testMalformedSTR = "mama mila ramu";
    private final String testSTR = "10;50;1;1647033792";

    private final String testMalformedSTR2= "mama mila ramu";
    private final String sectorName = "sec_0";
    private final String sectorName2 = "sec_5";

    private List<IntWritable> values;

    @Before
    public void setUp() {
        values = new ArrayList<IntWritable>();
        values.add(new IntWritable(7));
        values.add(new IntWritable(5));
        URI[] cacheFiles = new URI[2];
        cacheFiles[0] = new Path("sectors.json").toUri();
        cacheFiles[1] = new Path("temperature.json").toUri();
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapDriver.setCacheFiles(cacheFiles);
        reduceDriver.setCacheFiles(cacheFiles);
    }

    @Test
    public void testMapperCounterOne() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(testMalformedSTR))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounterZero() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testSTR))
                .withOutput(new Text(sectorName), new IntWritable(1))
                .runTest();
        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testSTR))
                .withInput(new LongWritable(), new Text(testMalformedSTR))
                .withInput(new LongWritable(), new Text(testMalformedSTR))
                .withOutput(new Text(sectorName), new IntWritable(1))
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testReducerCounterZero() throws IOException {
        List<IntWritable> zero = new ArrayList<>();
        zero.add(new IntWritable(0));
        reduceDriver
                .withInput(new Text(testMalformedSTR2), zero)
                .runTest();

        assertEquals("Expected 0 counter increment", 0, reduceDriver.getCounters()
                .findCounter(CounterType.ACTIVE_SECTORS).getValue());
    }

    /**
     * Reduce 1 active sector
     *
     * @throws IOException
     */
    @Test
    public void testReducerCounterOne() throws IOException {
        reduceDriver
                .withInput(new Text(sectorName), values)
                .withOutput(new Text(sectorName), new IntWritable(1))
                .runTest();

        assertEquals("Expected 1 counter increment", 1, reduceDriver.getCounters()
                .findCounter(CounterType.ACTIVE_SECTORS).getValue());
    }

    /**
     * Reduce 2 active sectors
     *
     * @throws IOException
     */
    @Test
    public void testReducerCounters() throws IOException {
        reduceDriver
                .withInput(new Text(sectorName), values)
                .withInput(new Text(sectorName2), values)
                .withOutput(new Text(sectorName), new IntWritable(1))
                .withOutput(new Text(sectorName2), new IntWritable(1))
                .runTest();

        assertEquals("Expected 2 counter increment", 2, reduceDriver.getCounters()
                .findCounter(CounterType.ACTIVE_SECTORS).getValue());
    }
}

