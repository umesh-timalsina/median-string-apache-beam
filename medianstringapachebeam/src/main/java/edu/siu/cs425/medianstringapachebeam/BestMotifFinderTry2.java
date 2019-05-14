package edu.siu.cs425.medianstringapachebeam;

import java.util.HashMap;

import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.siu.cs425.medianstringapachebeam.BestMotifFinder.Motifs;

public class BestMotifFinderTry2 extends CombineFn<KV<String, Integer>, HashMap<String, Integer>, KV<String, Integer>>{
	
	private String bestMotif = "";
	private Integer bestDistance = Integer.MAX_VALUE;
	private static final Logger logger = LoggerFactory.getLogger(BestMotifFinderTry2.class);

	@Override
	public HashMap<String, Integer> createAccumulator() {
		// TODO Auto-generated method stub
		return new HashMap<String, Integer>();
	}

	@Override
	public HashMap<String, Integer> addInput(HashMap<String, Integer> motifAccumalutor, KV<String, Integer> input) {
		motifAccumalutor.put(input.getKey(), input.getValue());
		logger.info("Executing addInput");
		return motifAccumalutor;
		
	}

	@Override
	public HashMap<String, Integer> mergeAccumulators(Iterable<HashMap<String, Integer>> motifAccumulators) {
		HashMap<String, Integer> allTargetMotifs = createAccumulator();
		logger.info("Executed merge accumulators");
		for(HashMap<String, Integer> motifAccumulator : motifAccumulators) {
			allTargetMotifs.putAll(motifAccumulator);
			
		}
		return allTargetMotifs;
	}

	@Override
	public KV<String, Integer> extractOutput(HashMap<String, Integer> motifAccumulator) {

		motifAccumulator.forEach((motif, distanceScore)->{
			if(distanceScore < this.bestDistance) {
				this.bestMotif = motif;
				this.bestDistance = distanceScore;
			}
		});
		logger.info("executed extract output");
		System.out.println("Best Motif: " + this.bestMotif + " Best Distance: " + this.bestDistance);
		return KV.of(this.bestMotif, this.bestDistance);
	}
}
