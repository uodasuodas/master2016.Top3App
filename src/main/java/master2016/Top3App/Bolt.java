package master2016.Top3App;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class Bolt extends BaseRichBolt {

    String keyword;
    String lang;
    Boolean started = false;
    Map<String, Integer> hashCount = new TreeMap<String, Integer>();

    public Bolt (String lang, String keyword){
        this.lang = lang;
        this.keyword = keyword;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
        String hashtag = (String) tuple.getValueByField("hashtag");
        String lang = (String) tuple.getValueByField("language");

        if (hashtag.equals(keyword)) {
            if (started == false) {
                started = true;
                System.out.println(lang + ", " + hashtag + " <---- START");
            } else {
                started = false;
                System.out.println(lang + ", " + hashtag + " <---- END");
                printResult();
                hashCount = new TreeMap<String, Integer>();
            }
        } else if (started == true) {
            count(hashtag);
            System.out.println(lang + ", " + hashtag);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void count(String hashtag) {
        if (hashCount.containsKey(hashtag)) {
            Integer count = hashCount.get(hashtag);
            count++;
            hashCount.put(hashtag, count);
        } else {
            hashCount.put(hashtag, 1);
        }
    }

    public void printResult () {
        hash firstHash = new hash(0, "");
        hash secondHash = new hash(0, "");
        hash thirdHash = new hash(0, "");

        for (Map.Entry<String, Integer> entry : hashCount.entrySet()) {
            String hashtag = entry.getKey();
            Integer count = entry.getValue();
            if (count > firstHash.count) {
                thirdHash.count = secondHash.count;
                thirdHash.hashtag = secondHash.hashtag;
                secondHash.count = firstHash.count;
                secondHash.hashtag = firstHash.hashtag;
                firstHash.count = count;
                firstHash.hashtag = hashtag;
            } else if (count > secondHash.count) {
                thirdHash.count = secondHash.count;
                thirdHash.hashtag = secondHash.hashtag;
                secondHash.count = count;
                secondHash.hashtag = hashtag;
            } else if (count > thirdHash.count) {
                thirdHash.count = count;
                thirdHash.hashtag = hashtag;
            }
        }
        System.out.print("#" + firstHash.hashtag + "," + firstHash.count + ", ");
        System.out.print("#" + secondHash.hashtag + "," + secondHash.count + ", ");
        System.out.print("#" + thirdHash.hashtag + "," + thirdHash.count);
    }

    private class hash {
        private int count = 0;
        private String hashtag = "";
        hash(int x, String y) {
            this.count=x;
            this.hashtag=y;
        }
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue( Map<K, V> map ) {

        List<Map.Entry<K, V>> list =
                new LinkedList<Map.Entry<K, V>>( map.entrySet() );
        Collections.sort( list, new Comparator<Map.Entry<K, V>>()
        {
            public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
            {
                return (o2.getValue()).compareTo( o1.getValue() );
            }
        } );

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list)
        {
            result.put( entry.getKey(), entry.getValue() );
        }
        return result;
    }
}