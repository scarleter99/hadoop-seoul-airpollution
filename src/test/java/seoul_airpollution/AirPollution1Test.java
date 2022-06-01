package seoul_airpollution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class AirPollution1Test {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInt("mapreduce.job.reduces", 3);

        //String[] input_args = {"src/test/resources/Measurement_info.csv"};
        String[] input_args = {"src/test/resources/abc.csv"};
        ToolRunner.run(conf, new AirPollution1(), input_args);
    }
}
