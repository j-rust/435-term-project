package com.vientiuno.tweet.count;

import org.apache.hadoop.io.IntWritable;
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
 * Created by Jonathan Rust on 4/5/16.
 */
public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private String TWITTER_DATE_FORMAT = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";

    /**
     * This mapper depends on three arguments passed into the configuration during job creation:
     *      1. key = start_time, value = date string with seconds set to 0
     *      2. key = end_time, value = date string with seconds set to 0
     *      3. key = keywords, value = comma separated list of terms/phrases
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
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
                String tweet_content = json.getString("text").toLowerCase();
                String keywords = context.getConfiguration().get("keywords").toLowerCase();
                String[] keyword_set = keywords.split(", ");
                for (String keyword : keyword_set) {
                    if (tweet_content.contains(keyword)) {
                        context.write(new Text(tweet_timstamp.toString()), new IntWritable(1));
                        break;
                    }
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

}
