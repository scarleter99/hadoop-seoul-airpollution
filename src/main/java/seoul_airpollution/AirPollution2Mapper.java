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
        // 0.날짜 시간, 1.지역, 2.물질, 3.수치 4.측정상태
        String[] line = value.toString().split(",");

        // 헤드 및 결측치/이상값 처리
        if (head || line[3].equals("-1") || !line[4].equals("0")) {
            head = false;
            return;
        }

        // 물질이 PM10 또는 PM2.5이고 공기질이 좋은 지역 데이터 출력 (key: 지역명, value: 1)
        if (line[2].equals("8") && Float.parseFloat(line[3]) <= 40
                || line[2].equals("9") && Float.parseFloat(line[3]) <= 15 ) {
            region.set(line[1]);
            context.write(region, one);
        }
    }
}
