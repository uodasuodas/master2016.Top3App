package master2016.Top3App;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Spout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    public static final String STREAMNAME = "hashtagstream";
    public static final String LANG = "language";
    public static final String FIELDNAME = "hashtag";

    public static List<String> langList = new ArrayList<String>();

    public Spout (List<String> langList) {
        this.langList = langList;
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
    }

    public void nextTuple() {
        //String lang = "lang"+new Random().nextInt(5);
        String lang = "en";
        String hashtag = String.valueOf(new Random().nextInt(10));
        Values values = new Values(lang, hashtag);
        collector.emit(STREAMNAME,values);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAMNAME, new Fields(LANG, FIELDNAME));
    }
}
