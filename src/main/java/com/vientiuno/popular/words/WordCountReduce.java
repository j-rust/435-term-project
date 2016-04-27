package com.vientiuno.popular.words;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Jonathan Rust on 4/24/16.
 */
public class WordCountReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> word_map = new HashMap<String, Integer>();
        for (Text t : values) {
            if (word_map.containsKey(t.toString())) {
                word_map.put(t.toString(), word_map.get(t.toString()) + 1);
            } else {
                word_map.put(t.toString(), 1);
            }
        }
        String top5 = "";
        for(int i = 0; i < 5; i++) {
            /* Using Java 8 features to find key associated to max value */
            HashMap.Entry<String, Integer> max_pair
                    = word_map
                    .entrySet()
                    .stream()
                    .max(HashMap.Entry.comparingByValue())
                    .get();
            top5 += max_pair.getKey() + "\t";
            word_map.remove(max_pair.getKey());
        }
        context.write(key, new Text(top5.trim()));

    }

}
