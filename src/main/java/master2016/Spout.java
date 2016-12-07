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

    public static final String Stream = "stream";
    public static final String FieldName1 = "language";
    public static final String FieldName2 = "hashtag";
    
    private SpoutOutputCollector Collector;
    private String BrokerUrl;
    private Properties Props;
    private KafkaConsumer< String, String > Consumer;
    private String Lang;

    public Spout( String brokerUrl, String lang ) {
    	
        this.BrokerUrl = brokerUrl;
        this.Lang = lang;
        
    }

    public void open( Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        
    	this.Collector = spoutOutputCollector;
        Props = new Properties();
        Props.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BrokerUrl );
        Props.put( "group.id", "Group1" );
        Props.put( "enable.auto.commit", "true" );
        Props.put( "auto.commit.interval.ms", "1000" );
        Props.put( "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" );
        Props.put( "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" );

        Consumer = new KafkaConsumer<String, String>( Props );
        Consumer.subscribe( Arrays.asList( Lang ) );

    }

    public void nextTuple() {
    	
        try {
        	
                ConsumerRecords< String, String > records = Consumer.poll( 10 );
                for ( ConsumerRecord< String, String > record : records ) {

                    String lang = record.key();
                    String hashtag = record.value();
                    Values values = new Values( lang, hashtag );
                    Collector.emit( Stream, values );
                    
                }
                
        } catch ( Exception e ) {
        	
            e.printStackTrace();
        
        }

    }

    public void declareOutputFields( OutputFieldsDeclarer outputFieldsDeclarer ) {

        outputFieldsDeclarer.declareStream( Stream, new Fields( FieldName1, FieldName2 ) );

    }
}
