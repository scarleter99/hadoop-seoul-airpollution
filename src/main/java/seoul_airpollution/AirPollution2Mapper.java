package seoul_airpollution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirPollution2Mapper extends Mapper<Object, Text, Text, IntWritable> {

    Boolean head = true;
    Text region = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // 0.날짜 시간, 1.지역, 2.오염물질, 3.수치
        String[] line = value.toString().split(",");

        if (head || line[3].equals("-1") || line[3].equals("0")) {
            head = false;
            return;
        }

        if (line[2].equals("8") && Float.parseFloat(line[3]) <= 40
                || line[2].equals("9") && Float.parseFloat(line[3]) <= 15 ) {
            region.set(line[1]);
            context.write(region, one);
        }
    }
}
