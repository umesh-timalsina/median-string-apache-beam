package edu.siu.cs425.medianstringapachebeam;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class SumDistances implements SerializableFunction<Iterable<Integer>, Integer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4284243676261828131L;

	@Override
	public Integer apply(Iterable<Integer> distances) {
		Integer totalDistance = 0;
		for (Integer distance : distances) {
			totalDistance += distance;
		}
		return totalDistance;
	}

}
