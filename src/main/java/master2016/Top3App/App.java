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
    private static List<String> langList = Arrays.asList("en:Entrepreneur", "es:madrid");
    public static void main( String[] args ) {
        TopologyBuilder builder = new TopologyBuilder();

        for (String langKey : langList) {
            String[] parts = langKey.split(Pattern.quote(":"));
            String lang = parts[0];
            String keyword = parts[1];
            builder.setSpout("Spout_" + lang, new Spout(lang));
            String boltName = "Bolt_" + lang;

            builder.setBolt(boltName, new Bolt(lang, keyword))
                    .shuffleGrouping("Spout_" + lang, Spout.STREAM);
        }
        LocalCluster lcluster = new LocalCluster();
        lcluster.submitTopology("HashtagTopology", new Config(), builder.createTopology());

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
