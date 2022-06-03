package seoul_airpollution;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirPollution1Mapper extends Mapper<Object, Text, Text, FloatWritable> {

    Boolean head = true;
    Text region = new Text();
    FloatWritable figure = new FloatWritable();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, FloatWritable>.Context context)
            throws IOException, InterruptedException {
        // 0.날짜 시간, 1.지역, 2.오염물질, 3.수치 4.측정상태
        String[] line = value.toString().split(",");

        // 헤드 및 결측치/이상값 처리
        if (head || line[3].equals("-1") || !line[4].equals("0")) {
            head = false;
            return;
        }

        // 오염물질명이 PM10인 데이터 출력 (key: 지역명, value: 수치)
        if (line[2].equals("8")) {
            region.set(line[1]);
            figure.set(Float.parseFloat(line[3]));
            context.write(region, figure);
        }
        head = false;
    }
}
