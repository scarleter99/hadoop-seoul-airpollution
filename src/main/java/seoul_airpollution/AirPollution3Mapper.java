package seoul_airpollution;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirPollution3Mapper extends Mapper<Object, Text, Text, FloatWritable> {

    Boolean head = true;
    Text dateTime_region = new Text();
    FloatWritable figure = new FloatWritable();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, FloatWritable>.Context context)
            throws IOException, InterruptedException {
        // 0.날짜 시간, 1.지역, 2.오염물질, 3.수치 4.측정상태
        String[] line = value.toString().split(",");

        // 헤드 처리
        if (head) {
            head = false;
            return;
        }

        // 모든 데이터 출력 (key: 날짜 시간 지역, value: 수치)
        dateTime_region.set(line[0] + "\t" + line[1]);
        figure.set(Float.parseFloat(line[3]));
        context.write(dateTime_region, figure);

    }
}
