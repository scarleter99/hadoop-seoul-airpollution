package seoul_airpollution;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirPollution1Reducer extends Reducer<Text, FloatWritable, Text, Text> {

    Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, Text>.Context context)
            throws IOException, InterruptedException {

        float sum = 0f;
        float max = 0f;
        float mean = 9999f;
        int num = 0;
        for(FloatWritable v : values) {
            float figure = v.get();
            sum += figure;
            if (max < figure) {
                max = figure;
            } else if (mean > figure){
                mean = figure;
            }
            num++;
        }

        // key: 지역명, value: 평균 최대 최소
        result.set(sum/num + "\t" + max + "\t" + mean);
        context.write(key, result);
    }
}
