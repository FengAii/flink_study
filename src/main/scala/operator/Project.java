package operator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Project {

    public static void main(String[] args) throws Exception {
        //生成配置对象
        Configuration config = new Configuration();
        //开启spark-webui
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        //配置webui的日志文件，否则打印日志到控制台
        config.setString("web.log.path", "/tmp/flink_log");
        //配置taskManager的日志文件，否则打印日志到控制台
        config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "/tmp/flink_log");
        //配置tm有多少个slot
        config.setString("taskmanager.numberOfTaskSlots", "12");

        // 获取local运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        DataStreamSource<Tuple4<String, String, String, Integer>> input = env.fromElements(TRANSCRIPT);

        //选择了第一列和第二列，project中的列是从0开始计算的
        DataStream<Tuple2<String, String>> out = input.project(1, 2);

        out.print();

        env.execute();

    }

    public static final Tuple4[] TRANSCRIPT = new Tuple4[]{
            Tuple4.of("hainiu", "class12", "小王", 50),
            Tuple4.of("hainiu", "class12", "小李", 55),
            Tuple4.of("hainiu", "class11", "小张", 50),
            Tuple4.of("hainiu", "class11", "小强", 45)
    };
}
