package com.vientiuno.popular.words;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Jonathan Rust on 4/24/16.
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, Text> {

    private String TWITTER_DATE_FORMAT = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";

    private HashSet<String> STOP_WORDS = new HashSet<String>(Arrays.asList("a", "about", "above", "after", "again", "against",
            "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below",
            "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't",
            "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
            "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him",
            "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
            "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on",
            "once", "only", "or", "other", "ought", "our", "ours	ourselves", "out", "over", "own", "rt", "same", "shan't", "she",
            "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their",
            "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've",
            "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're",
            "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's",
            "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your",
            "yours", "yourself", "yourselves", "finalfour", "#finalfour", "@marchmadness", "#marchmadness", "picking",
            "#nationalchampionship", "@bleacherreport:", "like", "&amp;"));

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
                Set seconds to 0 to get words/minute. Look for a cleaner way to do this
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
                    if(!STOP_WORDS.contains(word))
                        context.write(new Text(tweet_timstamp.toString()), new Text(word));
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
