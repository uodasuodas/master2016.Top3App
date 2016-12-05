package master2016;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class Top3App {

    private static List<String> langList = Arrays.asList("en:home", "es:ordenador");
    private static String brokerUrl = "localhost:9093,localhost:9094";
    private static String topologyName = "HashtagTopology";
    private static String folder = "";

    public static void main( String[] args ) {
        if (args.length > 0) {
            langList = Arrays.asList(args[0].split(Pattern.quote(",")));
            brokerUrl = args[1];
            topologyName = args[2];
            folder = args[3];
        }

        System.out.println(langList);
        System.out.println(brokerUrl);
        System.out.println(topologyName);
        System.out.println(folder);

        TopologyBuilder builder = new TopologyBuilder();

        for (String langKey : langList) {
            String[] parts = langKey.split(Pattern.quote(":"));
            String lang = parts[0];
            String keyword = parts[1];
            builder.setSpout("Spout_" + lang, new Spout(brokerUrl, lang));
            String boltName = "Bolt_" + lang;

            builder.setBolt(boltName, new Bolt(lang, keyword, folder))
                    .shuffleGrouping("Spout_" + lang, Spout.STREAM);
        }

        LocalCluster lcluster = new LocalCluster();
        lcluster.submitTopology(topologyName, new Config(), builder.createTopology());

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
