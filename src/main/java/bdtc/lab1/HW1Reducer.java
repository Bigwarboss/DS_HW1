package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.IOUtils;
import java.nio.charset.StandardCharsets;
import java.io.InputStream;
import java.io.FileInputStream;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Редьюсер: суммирует все единицы полученные от {@link HW1Mapper}, выдаёт суммарное количество пользователей по браузерам
 */

public class HW1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private TreeMap<Integer, Integer> temperatureMap = new TreeMap<Integer, Integer>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI file : cacheFiles) {
                    Path cacheFile = new Path(file);
                    if (cacheFile.getName().toUpperCase().contains("TEMPERATURE")) {
                        readFile(cacheFile);
                    }
                }
            }
        } catch (IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    private Integer getTemperature(Integer clicks) {
        Map.Entry<Integer, Integer> entry = temperatureMap.floorEntry(clicks);
        if (entry != null && entry.getValue() == null) {
            entry = temperatureMap.lowerEntry(clicks);
        }
        return entry == null ? null : entry.getValue();
    }

    private void readFile(Path filePath) {
        try {
            InputStream is = new FileInputStream(filePath.toString());
            String jsonTxt = IOUtils.toString(is, StandardCharsets.UTF_8);
            JSONObject json = new JSONObject(jsonTxt);
            Iterator<String> intItr = json.keys();
            while (intItr.hasNext()) {
                String clicksName = intItr.next();
                int clicks = Integer.parseInt(clicksName);
                temperatureMap.put(clicks, (Integer) json.get(clicksName));
            }
        } catch (IOException ex) {
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
        }
        if (sum > 0) {
            Integer temperature = getTemperature(sum);
            context.getCounter(CounterType.ACTIVE_SECTORS).increment(1);
            context.write(key, new IntWritable(temperature));
        }
    }
}
