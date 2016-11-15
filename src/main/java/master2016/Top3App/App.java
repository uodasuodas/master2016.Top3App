package master2016.Top3App;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    private static List<String> langList = Arrays.asList("lang1", "lang2", "lang3", "lang4", "lang0");
    public static void main( String[] args )
    {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Spout", new Spout(langList));


        builder.setBolt("Bolt", new Bolt("en", "5"))
                .shuffleGrouping("Spout",Spout.STREAMNAME);

        LocalCluster lcluster = new LocalCluster();
        lcluster.submitTopology("HashtagTopology", new Config(), builder.createTopology());

        //LEAVING IT RUN FOR 30 SECONDS...
        Utils.sleep(60000);

        lcluster.shutdown();

    }
}
