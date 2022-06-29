package org.example.salaryAverageLocation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class salaryAverageLocationMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable WritableValue = new IntWritable(0);
    private Text jobTitleText = new Text();
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();

        String[] split = line.split(",");
        String jobTitle = split[5].trim();
        System.out.println(jobTitle);
        String salarySplit = split[3];
        System.out.println(salarySplit);
        this.jobTitleText.set(jobTitle);
        if (salarySplit.matches("\\d+")){
            this.WritableValue.set(Integer.parseInt(salarySplit));
            output.collect(this.jobTitleText, this.WritableValue);
        }

    }
}