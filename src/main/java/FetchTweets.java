import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.map.deser.JsonNodeDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.StringTokenizer;

public class FetchTweets {
    private static final Logger LOG = LoggerFactory.getLogger(FetchTweets.class);

    public static void main(String arg[]) throws Exception {
        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "");
        props.setProperty(TwitterSource.TOKEN, "");
        props.setProperty(TwitterSource.TOKEN_SECRET, "");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> streamSource = env.addSource(new TwitterSource(props));

        DataStream<JsonNode> tweetStream = streamSource.map(new MapFunction<String, JsonNode>() {
            //retrieving json object out of json string
            @Override
            public JsonNode map(String s) throws Exception {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readValue(s,JsonNode.class);
                return jsonNode;
            }
        }).filter(new FilterFunction<JsonNode>() {
            // getting only those tweets which are in english
            @Override
            public boolean filter(JsonNode jsonNode) throws Exception {
                if(jsonNode.has("lang")
                        && jsonNode.get("lang").asText().equals("en")) {
                    return true;
                }
                    return false;
            }
        });

        //Sending tweets to Apache kafka

        FlinkKafkaProducer010<JsonNode> myProducer = new FlinkKafkaProducer010<JsonNode>(
                "localhost:9092",            // broker list
                "tweets",                  // target topic
                new JsonSerializer());
        myProducer.setWriteTimestampToKafka(true);
        tweetStream.addSink(myProducer);

        env.execute("TwitterExample");
    }
}
