package seoul_airpollution;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirPollution4Mapper extends Mapper<Object, Text, Text, Text> {

    Boolean head = true;
    Text time = new Text();
    Text material_figure = new Text();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        // 0.날짜 시간, 1.지역, 2.물질, 3.수치 4.측정상태
        String[] line = value.toString().split(",");

        // 헤드 및 결측치/이상값 처리
        if (head || line[3].equals("-1") || !line[4].equals("0")) {
            head = false;
            return;
        }

        // 모든 데이터 출력 (key: 시간 , value: 물질 수치)
        time.set(line[0].split(" ")[1]);
        material_figure.set(line[2] + "\t" + line[3]);
        context.write(time, material_figure);
    }
}
