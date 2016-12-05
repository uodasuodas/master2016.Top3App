package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class Bolt extends BaseRichBolt {

    String keyword;
    String lang;
    String folder;
    Boolean started = false;
    Integer counter = 0;
    Map<String, Integer> hashCount = new TreeMap<String, Integer>();

    Bolt(String lang, String keyword, String folder){
        this.lang = lang;
        this.keyword = keyword;
        this.folder = folder;
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
                counter++;
                System.out.println(lang + ", " + hashtag + " <---- END");
                printResult();
                hashCount = new TreeMap<String, Integer>();
            }
        } else if (started == true) {
            count(hashtag);
            System.out.println(lang + ", " + hashtag);
        } else {
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
        hash firstHash = new hash(0, "null");
        hash secondHash = new hash(0, "null");
        hash thirdHash = new hash(0, "null");

        List<hash> hashList = Arrays.asList(firstHash, secondHash, thirdHash);

        for (Map.Entry<String, Integer> entry : hashCount.entrySet()) {
            hash newHash = new hash(entry.getValue(), entry.getKey());
            if (newHash.count > hashList.get(0).count) {
                hashList.set(2, hashList.get(1));
                hashList.set(1, hashList.get(0));
                hashList.set(0, newHash);
            } else if (newHash.count == hashList.get(0).count) {
                int compare = newHash.hashtag.compareTo(hashList.get(0).hashtag);
                if (compare < 0) {
                    hashList.set(2, hashList.get(1));
                    hashList.set(1, newHash);
                } else if (compare > 0) {
                    hashList.set(2, hashList.get(1));
                    hashList.set(1, hashList.get(0));
                    hashList.set(0, newHash);
                }
            } else if (newHash.count > hashList.get(1).count) {
                hashList.set(2, hashList.get(1));
                hashList.set(1, newHash);
            } else if (newHash.count == hashList.get(1).count) {
                int compare = newHash.hashtag.compareTo(hashList.get(1).hashtag);
                if (compare < 0) {
                    hashList.set(2, newHash);
                } else if (compare > 0) {
                    hashList.set(2, hashList.get(1));
                    hashList.set(1, newHash);
                }
            } else if (newHash.count > hashList.get(2).count) {
                hashList.set(2, newHash);
            } else if (newHash.count == hashList.get(2).count) {
                int compare = newHash.hashtag.compareTo(hashList.get(1).hashtag);
                if (compare > 0) {
                    hashList.set(2, newHash);
                }
            }
        }

        resultToFile(hashList);
    }


    public void resultToFile (List<hash> hashList) {
        try (
            FileWriter fw = new FileWriter(lang + "_05", true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw)
        ){
            out.print(counter + "," + lang + ",");
            for (hash hash : hashList) {
                out.print(hash.hashtag + "," + hash.count + ",");
            }
            out.println();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    private class hash {
        private int count = 0;
        private String hashtag = "";
        hash(int x, String y) {
            this.count=x;
            this.hashtag=y;
        }
    }

}