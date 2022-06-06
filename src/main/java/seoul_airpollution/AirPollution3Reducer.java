package seoul_airpollution;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class AirPollution3Reducer extends Reducer<Text, Text, Text, Text> {

    Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {

        String[] figures = new String[6];
        for(Text v : values) {
            String[] vArr = v.toString().split("\t");
            switch (vArr[0]) {
                case "1":
                    figures[0] = vArr[1];
                    break;
                case "3":
                    figures[1] = vArr[1];
                    break;
                case "5":
                    figures[2] = vArr[1];
                    break;
                case "6":
                    figures[3] = vArr[1];
                    break;
                case "8":
                    figures[4] = vArr[1];
                    break;
                case "9":
                    figures[5] = vArr[1];
                    break;
            }
        }

        // key: 날짜 시간 지역, value: 모든 물질 수치
        result.set(String.join("\t", figures));
        context.write(key, result);
    }
}
