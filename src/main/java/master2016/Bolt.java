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
    private String Folder;
    private Boolean Started = false;
    private Integer Counter = 0;
    private Map< String, Integer > HashCount = new TreeMap< String, Integer >();

    public Bolt( String lang, String keyword, String folder )
    {
        this.Lang = lang;
        this.Keyword = keyword;
        this.Folder = folder;
    }
    
    public void prepare( Map map, TopologyContext topologyContext, OutputCollector outputCollector ){}

    
    public void execute( Tuple tuple )
    {
    	String lang = ( String )tuple.getValueByField( Spout.FieldName1 );
        String hashtag = ( String )tuple.getValueByField( Spout.FieldName2 );      
        if( hashtag.equals( Keyword ) )
        {
            if( Started == false )
            {
                Started = true;
                System.out.println( lang + ", " + hashtag + " -> start (" + lang + ")" );
            }
            else
            {	
                Started = false;
                Counter++;
                System.out.println( lang + ", " + hashtag + " -> stop (" + lang + ")" );
                printResult();
                HashCount = new TreeMap< String, Integer >();
            }
        }
        else
        {
        	System.out.println( lang + ", " + hashtag );
        	if( Started == true )    
        		count( hashtag );
        }
    }

    
    public void declareOutputFields( OutputFieldsDeclarer outputFieldsDeclarer ){}

    
    private void count( String hashtag )
    {
        if( HashCount.containsKey( hashtag ) )
        {
        	Integer count = HashCount.get( hashtag );
            count++;
            HashCount.put( hashtag, count );
        }
        else
        	HashCount.put( hashtag, 1 );
    }

    
    private void printResult()
    {
        List< hash > hashList = Arrays.asList( new hash( 0, "null" ), new hash( 0, "null" ), new hash( 0, "null" ) );        
        for( Map.Entry< String, Integer > entry : HashCount.entrySet() )
        {
            hash newHash = new hash( entry.getValue(), entry.getKey() );
            if( newHash.count == hashList.get( 0 ).count ||
            		newHash.count == hashList.get( 1 ).count )
            	insertFollowingCountAndAlph( hashList, newHash );
            
            else if( newHash.count > hashList.get( 0 ).count )
            	insertInPos( hashList, newHash, 0 );
            
            else if( newHash.count > hashList.get( 1 ).count )
            	insertInPos( hashList, newHash, 1 );
            
            else if( newHash.count > hashList.get( 2 ).count )
            	hashList.set( 2, newHash );
            
            else if( newHash.count == hashList.get( 2 ).count )
            {
            	int compare = newHash.hashtag.toLowerCase().compareTo( hashList.get( 2 ).hashtag.toLowerCase() );
                if( compare < 0 )
                	hashList.set( 2, newHash );
            }
        }
        resultToFile( hashList );
    }
    
    private void insertFollowingCountAndAlph( List< hash > list, hash newElement ) {
    	
    	boolean stop = false;
    	int i = 0;
    	while( !stop && i < list.size() )
    	{
    		if( list.get( i ).count == newElement.count )
    		{
    			int compare = newElement.hashtag.toLowerCase().compareTo( list.get( i ).hashtag.toLowerCase() );
    			if( compare < 0 )
    			{
    				insertInPos( list, newElement, i);
    				stop = true;
    			}
    		}
    		else if( list.get( i ).count < newElement.count )
    		{
    			insertInPos( list, newElement, i);
				stop = true;
    		}
    		i++;
    	}
    }
    
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

    private void resultToFile(List<hash> hashList)
    {
    	String path = System.getProperty( "user.dir" );
    	if( !Folder.startsWith( "/", 0 ) )
    		path = path + "/" + Folder;
    	else
    		path = path + Folder;
    	File folder = new File( path );
    	if( !folder.exists() )
    	{
    		if ( !folder.mkdirs() )
    			System.out.println("Not able to create directory: " + path );
    	}
    	if( folder.exists() && folder.isDirectory() )
    	{
    		if( !path.endsWith("/") )
	    		path = path + "/";
    		try(
    				FileWriter fw = new FileWriter( path + Lang + "_05.log", true);
    	            BufferedWriter bw = new BufferedWriter( fw );
    	            PrintWriter out = new PrintWriter( bw )	
    	        )
    		{
    			out.print( Counter + "," + Lang + "," );
    			for( hash hash : hashList )
    			{
    				out.print( hash.hashtag + "," + hash.count );
    				if( hash != hashList.get( hashList.size() - 1 ) )
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
   		 System.out.println( "Path( " + path + " ) does not define a folder");
    }

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