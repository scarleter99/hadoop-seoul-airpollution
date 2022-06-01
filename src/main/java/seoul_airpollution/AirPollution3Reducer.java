package seoul_airpollution;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirPollution3Reducer extends Reducer<Text, FloatWritable, Text, Text> {

    Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, Text>.Context context)
            throws IOException, InterruptedException {

        StringBuilder figures = new StringBuilder();
        for(FloatWritable v : values) {
            figures.append(v.get()).append("\t");
        }
        figures = new StringBuilder(figures.toString().trim());

        result.set(figures.toString());
        context.write(key, result);
    }
}
