package master2016;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class Top3App
{
    private static List<String> LangList;//= Arrays.asList( "en:home", "es:ordenador" );
    private static String BrokerUrl;// = "138.4.110.141:9092";
    private static String TopologyName;// = "HashtagTopology";
    private static String Folder;// = "";

    public static void main( String[] args ) throws Exception
    {
    	if( args.length != 4 )
    		throw new Exception( "The topology must have four arguments" );
    	    	
    	LangList = Arrays.asList( args[ 0 ].split( Pattern.quote( "," ) ) );
    	BrokerUrl = args[ 1 ];
    	TopologyName = args[ 2 ];
    	Folder = args[ 3 ];
        TopologyBuilder builder = new TopologyBuilder();
        for( String langKey : LangList )
        {
            String[] parts = langKey.split( Pattern.quote( ":" ) );
            String lang = parts[ 0 ];
            String keyword = parts[ 1 ];
            
            String spoutName = "Spout_" + lang;
            builder.setSpout( spoutName, new Spout( BrokerUrl, lang ) );
            
            String boltName = "Bolt_" + lang;
            builder.setBolt( boltName, new Bolt( lang, keyword, Folder ) )
                    .shuffleGrouping( "Spout_" + lang, Spout.Stream );
        }

        Config config = new Config();
		config.setNumWorkers( 3 );
		StormSubmitter.submitTopology( TopologyName, config, builder.createTopology() );
        //LocalCluster lcluster = new LocalCluster();
        //lcluster.submitTopology( TopologyName, new Config(), builder.createTopology() );
    }
}
