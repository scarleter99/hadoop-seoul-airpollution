package seoul_airpollution;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirPollution4Reducer extends Reducer<Text, FloatWritable, Text, Text> {

    Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, Text>.Context context)
            throws IOException, InterruptedException {

        float sum = 0f;
        int num = 0;
        for(FloatWritable v : values) {
            float figure = v.get();
            sum += figure;
            num++;
        }

        result.set(sum/num + "");
        context.write(key, result);
    }
}
