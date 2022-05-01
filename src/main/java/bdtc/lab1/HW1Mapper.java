package bdtc.lab1;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.IOUtils;

import java.nio.charset.StandardCharsets;
import java.io.IOException;

import java.io.InputStream;
import java.io.FileInputStream;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private HashMap<String, JSONArray> coordinateMap = new HashMap<String, JSONArray>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI file : cacheFiles) {
                    Path cacheFile = new Path(file);
                    if (cacheFile.getName().toUpperCase().contains("SECTORS")) {
                        readFile(cacheFile);
                    }
                }
            }
        } catch (IOException | JSONException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    private void readFile(Path filePath) throws JSONException {
        try {
            InputStream is = new FileInputStream(filePath.toString());
            String jsonTxt = IOUtils.toString(is, StandardCharsets.UTF_8);
            JSONObject json = new JSONObject(jsonTxt);
            Iterator<String> nameItr = json.keys();
            while (nameItr.hasNext()) {
                String name = nameItr.next();
                coordinateMap.put(name, (JSONArray) json.get(name));
            }
        } catch (IOException ex) {
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
        }
    }

    private String getSectorName(String x, String y) throws JSONException {
        int x_c = Integer.parseInt(x);
        int y_c = Integer.parseInt(y);
        for (String key : coordinateMap.keySet()) {
            JSONArray tmp = coordinateMap.get(key);
            JSONArray left = (JSONArray) tmp.get(0);
            JSONArray right = (JSONArray) tmp.get(1);
            if (x_c < 0 || y_c < 0) { continue; }
            if (x_c >= left.getInt(0) && x_c <= right.getInt(0) &&
                    y_c >= left.getInt(1) && y_c <= right.getInt(1)) {
                return key;
            }
        }
        return null;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] cords = line.split(";");
        if (cords.length < 2) {
            System.out.println(Arrays.toString(cords));
            context.getCounter(CounterType.MALFORMED).increment(1);
            return;
        }
        String sectorName = "";
        try {
            sectorName = getSectorName(cords[0], cords[1]);
        } catch (JSONException e) {
            e.printStackTrace();
        }

        if (sectorName == null) {
            System.out.println(Arrays.toString(cords));
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            context.write(new Text(sectorName), one);
        }
    }
}
