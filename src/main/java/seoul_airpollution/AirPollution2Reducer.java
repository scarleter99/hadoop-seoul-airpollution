package seoul_airpollution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirPollution2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable v : values) {
            sum += v.get();
        }

        // key: 지역명, value: 지역수
        result.set(sum);
        context.write(key, result);
    }
}
