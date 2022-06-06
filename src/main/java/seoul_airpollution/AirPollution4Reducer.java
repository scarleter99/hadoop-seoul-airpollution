package seoul_airpollution;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirPollution4Reducer extends Reducer<Text, Text, Text, Text> {

    Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {

        float[] figures = new float[6];
        int[] figuresNum = new int[6];
        for(Text v : values) {
            String[] vArr = v.toString().split("\t");
            switch (vArr[0]) {
                case "1":
                    figures[0] += Float.parseFloat(vArr[1]);
                    figuresNum[0]++;
                    break;
                case "3":
                    figures[1] += Float.parseFloat(vArr[1]);
                    figuresNum[1]++;
                    break;
                case "5":
                    figures[2] += Float.parseFloat(vArr[1]);
                    figuresNum[2]++;
                    break;
                case "6":
                    figures[3] += Float.parseFloat(vArr[1]);
                    figuresNum[3]++;
                    break;
                case "8":
                    figures[4] += Float.parseFloat(vArr[1]);
                    figuresNum[4]++;
                    break;
                case "9":
                    figures[5] += Float.parseFloat(vArr[1]);
                    figuresNum[5]++;
                    break;
            }
        }

        // key: 시간 오염물질명, value: 평균
        float[] averages = new float[6];
        for (int i = 0; i < 6; i++) {
            averages[i] += figures[i]/figuresNum[i];
        }

        StringBuilder averagesStr = new StringBuilder();
        for (float a : averages) {
            averagesStr.append(a).append("\t");
        }

        result.set(averagesStr.toString().trim());
        context.write(key, result);
    }
}
