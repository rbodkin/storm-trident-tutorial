// Copyright (c) 2013, Think Big Analytics.
package com.thinkbiganalytics.storm.trident;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/*-
 * Filter volume levels within the Storm Trident framework.
 * Inputs: volume
 * Outputs: kept tuple
 */
public class StockVolumeFilter extends BaseFilter {

	/**
	 *  modify the filter so that only tuples with a volume greater than a certain volume are kept
	 */
	long volume; 
	
	public StockVolumeFilter (long inVolume) {
		this.volume = inVolume;
	}
	
	@Override 
	public boolean isKeep(TridentTuple tuple) {
		return tuple.getLong(0) > volume; 
		}
	
}