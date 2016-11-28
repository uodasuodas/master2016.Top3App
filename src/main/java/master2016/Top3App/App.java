package master2016.Top3App;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class App {
    private static List<String> langList = Arrays.asList("lang1:1", "lang2:2", "lang3:3", "lang4:4", "lang5:5");
    public static void main( String[] args ) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("Spout", new Spout(returnLanguages(langList)));

        for (String langKey : langList) {
            String[] parts = langKey.split(Pattern.quote(":"));
            String lang = parts[0];
            String keyword = parts[1];
            String boltName = "Bolt_" + lang;
            builder.setBolt(boltName, new Bolt(lang, keyword))
                    .fieldsGrouping("Spout",lang, new Fields("language"));
        }
        LocalCluster lcluster = new LocalCluster();
        lcluster.submitTopology("HashtagTopology", new Config(), builder.createTopology());

        //LEAVING IT RUN FOR 60 SECONDS...
        Utils.sleep(500000);
        Spout.consumer.close();
        lcluster.shutdown();

    }

    public static List<String> returnLanguages (List<String> langs) {
        List<String> langList = new ArrayList<String>();
        for (String lang : langs) {
            String[] language = lang.split(Pattern.quote(":"));
            langList.add(language[0]);
        }
        return langList;
    }
}
