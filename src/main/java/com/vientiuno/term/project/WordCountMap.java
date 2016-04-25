package com.vientiuno.term.project;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * Created by Jonathan Rust on 4/24/16.
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, Text> {

    private String TWITTER_DATE_FORMAT = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(value.toString().trim().equals("")) return;

        try {
            String start_time_string = context.getConfiguration().get("start_time");
            String end_time_string = context.getConfiguration().get("end_time");

            JSONObject json = new JSONObject(value.toString());

            SimpleDateFormat sf = new SimpleDateFormat(TWITTER_DATE_FORMAT, Locale.ENGLISH);
            sf.setLenient(true);

            if(!json.has("created_at")) return;

            Date tweet_timstamp = sf.parse(json.getString("created_at"));
            /*
                Set seconds to 0 to count tweets/minute. Look for a cleaner way to do this
             */
            Calendar cal = Calendar.getInstance();
            cal.setTime(tweet_timstamp);
            cal.set(Calendar.SECOND, 0);
            tweet_timstamp = cal.getTime();

            Date start_timestamp = sf.parse(start_time_string);
            Date end_timestamp = sf.parse(end_time_string);

            if (tweet_timstamp.after(start_timestamp) && tweet_timstamp.before(end_timestamp)) {
                String[] tweet_content = json.getString("text").toLowerCase().split("\\s+");
                for (String word : tweet_content) {
                    context.write(new Text(tweet_timstamp.toString()), new Text(word));
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
