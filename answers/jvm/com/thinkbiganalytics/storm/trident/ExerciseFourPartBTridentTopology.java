// Copyright (c) 2013, Think Big Analytics.
package com.thinkbiganalytics.storm.trident;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ExerciseFourPartBTridentTopology {

    private static final int DRPC_QUERY_ITERATIONS = 2000;
    private static final int DRPC_QUERY_INTERVAL = 200;
    // Data path relative to pom.xml file.
    private static final String DATA_PATH = "data/20130301.csv.gz";

    /**
     * Launch a topology that reads stock symbols and prices from a CSV data file. Query the topology with DRPC every
     * 
     * @param args
     * @throws Exception
     */
    public static void main( String[] args ) throws Exception {
        Config conf = new Config();
        // conf.setDebug( true );
        conf.setMaxSpoutPending( 20 );

        // This topology can only be run as local because it is a toy example
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology( "symbolCounter", conf, buildTopology( drpc ) );

        
        for (int i = 0; i < DRPC_QUERY_ITERATIONS; i++) {
        	List<String> val = seen.get("INTC");
        	if (val != null) {
        		String last = val.get(val.size()-1);
        		System.err.println("Last minute seen for INTC: "+last);
	            System.err.println( "Result for DRPC stock volume query -> " + drpc.execute( "trades", last ) );
        	}
            Thread.sleep( DRPC_QUERY_INTERVAL );
        }

    }

    // hash map to simulate a NoSQL database
    private static final Map<String,List<String>> seen = new HashMap<String,List<String>>();
    
    public static class StoreSymbolMinute extends BaseFilter {

		@Override
		public boolean isKeep(TridentTuple tuple) {
			String symbolMinute = tuple.getStringByField("symbol_minute");
			String symbol = symbolMinute.split("\\|")[0];
			synchronized(seen) {
				List<String> minutes = seen.get(symbol);
				if (minutes == null) {
					minutes = new ArrayList<String>();
					seen.put(symbol, minutes);
				}
				if (!minutes.contains(symbolMinute)) {
					minutes.add(symbolMinute);
				}
			}
			return false;
		}
    	
    }
    
    public static StormTopology buildTopology( LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

        // Create spout to read data from CSV file and emit fields "date", "symbol", "price", "shares"
        // The Fields instance is used to name the fields the parsed output fields are mapped to.
        CSVBatchSpout spout = new CSVBatchSpout( DATA_PATH, new Fields( "date", "symbol", "price", "shares" ) );

        //
        // In this state we will save the real-time counts for each symbol
        // For this demo, we will use an in-process memory map. We could just as easily use Memcached or a NoSQL data store
        StateFactory mapState = new MemoryMapState.Factory();

        Stream stream = topology.newStream( "quotes", spout );
        
        // Real-time part of the system: a Trident topology that groups by symbol and stores per-symbol counts
        Stream streamWithSymbolMinute = stream
        //

                // Debug output of "quotes" spout
                //.each( new Fields( "date", "symbol", "price", "shares" ), new Debug() )

                // hour stamp added to key
                .each( new Fields( "symbol", "date"), new SymbolMinuteKey(), new Fields( "symbol_minute" ));
        
		TridentState tradeVolume = streamWithSymbolMinute
                
                //uncomment to debug
                //.each( new Fields( "symbol", "date", "symbol_minute"), new Debug())

                // -- fields grouping by "symbol" and "hour"
                .groupBy( new Fields( "symbol_minute" ) )

                //
                // Aggregate shares by symbol and hour, projecting new field "volume"
                .persistentAggregate( mapState, new Fields( "shares" ), new Sum(), new Fields( "volume" ) );
        
        streamWithSymbolMinute.
        	each(new Fields("symbol_minute"), new StoreSymbolMinute());
        
        /**
         * Now setup a DRPC stream on top of the "quotes" stream. The DRPC stream will generate a list of symbols and then query the
         * persistent aggregate state by symbol for the volume value.
         */
        topology.newDRPCStream( "trades", drpc )
        //

                // Freaking awesome DEBUG!!!
                // This debug statement will emit stock symbols: DEBUG: [INTC GE AAPL]
                // .each( new Fields( "args" ), new Debug() )

                // state query. The input is always "args", and needs to be split into individual fields
                // Split() implements tuple.getString(0).split(" ")
                .each( new Fields( "args" ), new Split(), new Fields( "symbol_minute" ) )
                //
                // .each( new Fields( "symbol_minute" ), new Debug() )
                //
                .groupBy( new Fields( "symbol_minute" ) )

                //
                // Query that persistent state that we setup earlier
                // Query key value field is "symbol" and result is projected as "volume"
                .stateQuery( tradeVolume, new Fields( "symbol_minute" ), new MapGet(), new Fields( "volume" ) )

                // remove nulls for 'each' value in the stream (aka Filtering)
                .each( new Fields( "volume" ), new FilterNull() )
     
                //
                // Debug print symbol and volume values
                // .each( new Fields( "symbol", "volume" ), new Debug() )

                // Remove this line to revert back to per stock symbol aggregate
                //.aggregate(new Fields("volume"), new Sum(), new Fields ("total_volume"))
          
                //
                // Project allows us to keep only the interesting fields that interest us
                //.project( new Fields( "symbol", "volume") );
                
                // Project total volume of trades for all stocks not just individuals
                //.project( new Fields("total_volume"))
                ;

        return topology.build();
    }
    
    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat( "yyyy-MM-dd-HH:mm:ss" );
    
    public static class SymbolMinuteKey extends BaseFunction {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			Date date = (Date)tuple.getValueByField("date");
			date = new Date(date.getTime()); // new object to not mutate original tuple!
		    //alternatively, just format with :00 instead of :ss
			date.setSeconds(0);
			String compoundKey = tuple.getStringByField("symbol")+"|"+FORMATTER.format(date);
			collector.emit(new Values(compoundKey));
		}
    	
    }
}
