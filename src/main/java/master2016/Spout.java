package master2016;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;

public class Spout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    public static final String STREAM = "stream";
    public static final String LANG = "language";
    public static final String FIELDNAME = "hashtag";
    public static String brokerUrl;
    public Properties props;
    public KafkaConsumer<String, String> consumer;

    public String lang;
    public static List<KafkaConsumer<String, String>> consumers = new ArrayList<KafkaConsumer<String, String>>();

    Spout (String brokerUrl, String lang) {
        this.brokerUrl = brokerUrl;
        this.lang = lang;
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put("group.id", "Group1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(lang));

    }

    public void nextTuple() {
        try{
                ConsumerRecords<String, String> records = consumer.poll(10);
                for (ConsumerRecord<String, String> record : records){

                    String lang = record.key();
                    String hashtag = record.value();

                    Values values = new Values(lang, hashtag);

                    collector.emit(STREAM,values);
                }
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream(STREAM, new Fields(LANG, FIELDNAME));

    }
}
