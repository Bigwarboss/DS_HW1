package bdtc.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import java.util.Objects;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class MapReduceApplication extends Configured implements Tool{
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(MapReduceApplication.class);
    private boolean debug = false;

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MapReduceApplication(), args);
        System.exit(exitCode);
    }

    /**
     * run function for ToolRunner
     */
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(MapReduceApplication.class);
        job.setJobName("Click Temperature Map");
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (debug) {
            job.setOutputFormatClass(TextOutputFormat.class);
        } else {
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
        }

        // coordinates
        job.addCacheFile(new Path(args[2]).toUri());

        // clicks
        job.addCacheFile(new Path(args[3]).toUri());

        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");

        // check counters
        Counter counterBad = job.getCounters().findCounter(CounterType.MALFORMED);
        log.info("=====================COUNTERS " + counterBad.getName() + ": " + counterBad.getValue()
                + "=====================");
        Counter counterGood = job.getCounters().findCounter(CounterType.ACTIVE_SECTORS);
        log.info("=====================COUNTERS " + counterGood.getName() + ": " + counterGood.getValue()
                + "=====================");
        return 0;
    }
}
