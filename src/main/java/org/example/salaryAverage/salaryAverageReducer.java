package org.example.salaryAverage;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class salaryAverageReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        int count = 0;
        int sum   =0;
        double average = 0;

        while(values.hasNext()) {
            count ++;
            IntWritable currentAmount = values.next();
            sum += currentAmount.get();
        }
        average = sum/count;
        output.collect(key, new IntWritable((int)average ));
    }
}


