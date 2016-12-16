package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import java.util.*;

public class Bolt extends BaseRichBolt
{
    private String Keyword;
    private String Lang;
    private String OutputFolder;
    private Boolean CountStarted = false;
    private Integer OutputLineCounter = 0;
    private Map< String, Integer > HashtagCountMap = new TreeMap< String, Integer >();

    public Bolt( String lang, String keyword, String folder )
    {
        this.Lang = lang;
        this.Keyword = keyword;
        this.OutputFolder = folder;
    }
    
    public void prepare( Map map, TopologyContext topologyContext, OutputCollector outputCollector ){}

    
    public void execute( Tuple tuple )
    {
    	String lang = ( String )tuple.getValueByField( Spout.FieldLangName );
        String hashtag = ( String )tuple.getValueByField( Spout.FieldHashtagName );      
        if( hashtag.equals( Keyword ) )
        {
            if( !CountStarted )
            {
            	CountStarted = true;
                System.out.println( lang + ", " + hashtag + " -> start (" + lang + ")" );
            }
            else
            {	
            	CountStarted = false;
            	OutputLineCounter++;
                System.out.println( lang + ", " + hashtag + " -> stop (" + lang + ")" );
                printResult();
                HashtagCountMap.clear();
            }
        }
        else
        {
        	System.out.println( lang + ", " + hashtag );
        	if( CountStarted )    
        		count( hashtag );
        }
    }

    
    public void declareOutputFields( OutputFieldsDeclarer outputFieldsDeclarer ){}

    /*
    *   Increases the counter in a HashCount structure of a given hashtag
    */
    private void count( String hashtag )
    {
        if( HashtagCountMap.containsKey( hashtag ) )
        {
        	Integer count = HashtagCountMap.get( hashtag );
            HashtagCountMap.put( hashtag, ++count );
        }
        else
        	HashtagCountMap.put( hashtag, 1 );
    }

    /*
    *   Prints top three hashtags by count (to a file) of a current HashCount structure
    */
    private void printResult()
    {
    	List< hash > top3hashtag = getTop3Hashtags();
    	if( !OutputFolder.startsWith( "/", 0 ) )
    		OutputFolder = "/" + OutputFolder;
    	File folder = new File( OutputFolder );
    	if( !folder.exists() )
    	{
    		if ( !folder.mkdirs() )
    			System.out.println("Not able to create directory: " + OutputFolder );
    	}
    	if( folder.exists() && folder.isDirectory() )
    	{
    		if( !OutputFolder.endsWith("/") )
    			OutputFolder = OutputFolder + "/";
    		try(
    				FileWriter fw = new FileWriter( OutputFolder + Lang + "_05.log", true);
    	            BufferedWriter bw = new BufferedWriter( fw );
    	            PrintWriter out = new PrintWriter( bw )	
    	        )
    		{
    			out.print( OutputLineCounter + "," + Lang + "," );
    			for( hash hash : top3hashtag )
    			{
    				out.print( hash.hashtag + "," + hash.count );
    				if( hash != top3hashtag.get( top3hashtag.size() - 1 ) )
    					out.print( "," );
    			}
    			out.println();
    		}
    		catch( IOException e )
    		{
    			System.out.println( e );
    		}
    	}
    	else if( folder.exists() && !folder.isDirectory() )
    		System.out.println( "Path( " + OutputFolder + " ) does not define a folder");
    }
    
    private List< hash > getTop3Hashtags()
    {
    	List< hash > hashList = Arrays.asList( new hash( 0, "null" ), new hash( 0, "null" ), new hash( 0, "null" ) );        
        for( Map.Entry< String, Integer > entry : HashtagCountMap.entrySet() )
        {
            hash newHash = new hash( entry.getValue(), entry.getKey() );
            if( newHash.count == hashList.get( 0 ).count ||
            		newHash.count == hashList.get( 1 ).count )
            	insertFollowingCountAndAlphOrder( hashList, newHash );
            
            else if( newHash.count > hashList.get( 0 ).count )
            	insertInPos( hashList, newHash, 0 );
            
            else if( newHash.count > hashList.get( 1 ).count )
            	insertInPos( hashList, newHash, 1 );
            
            else if( newHash.count > hashList.get( 2 ).count )
            	hashList.set( 2, newHash );
            
            else if( newHash.count == hashList.get( 2 ).count )
            {
                if( newHash.hashtag.toLowerCase().compareTo( 
                		hashList.get( 2 ).hashtag.toLowerCase() ) < 0 )
                	hashList.set( 2, newHash );
            }
        }
        return hashList;
    }

    /*
    *   Searches for an element in the list that has the same count as new element,
    *   compares hash strings and inserts new element based on alphabetical order
    *   @accepts List<hash>
    *   @accepts hash
    */
    private void insertFollowingCountAndAlphOrder( List< hash > list, hash newElement ) {
    	
    	boolean stop = false;
    	int i = 0;
    	while( !stop && i < list.size() )
    	{
    		if( list.get( i ).count == newElement.count &&
    				newElement.hashtag.toLowerCase().compareTo
    				( list.get( i ).hashtag.toLowerCase() ) < 0 )
    		{
    			insertInPos( list, newElement, i);
    			stop = true;
    		}
    		else if( list.get( i ).count < newElement.count )
    		{
    			insertInPos( list, newElement, i);
				stop = true;
    		}
    		i++;
    	}
    }

    /*
    *   Inserts new element into the list at a given position.
    *   @accepts List<hash>
    *   @accepts hash
    *   @accepts int
    */
    private void insertInPos( List< hash > list, hash newElement, int pos )
    {
    	if( pos >= 0 && pos < list.size() )
    	{
    		for( int i = list.size()-1; i > pos; i-- )
    		{
        		list.set(i, list.get( i - 1 ) );
    		}
        	list.set( pos, newElement );
    	}
    }

    /*
    *   Data structure for storing hashtag and its count
    */
    private class hash
    {
    	private int count = 0;
        private String hashtag = "";
        hash( int x, String y )
        {
            this.count=x;
            this.hashtag=y;
        }
    }
}