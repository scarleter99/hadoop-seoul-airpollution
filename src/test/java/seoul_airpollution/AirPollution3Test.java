package seoul_airpollution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class AirPollution3Test {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInt("mapreduce.job.reduces", 3);

        String[] input_args = {"src/test/resources/Measurement_info.csv"};
        ToolRunner.run(conf, new AirPollution3(), input_args);
    }
}
