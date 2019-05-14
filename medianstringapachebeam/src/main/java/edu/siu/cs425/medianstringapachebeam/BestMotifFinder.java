package edu.siu.cs425.medianstringapachebeam;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.KV;

public class BestMotifFinder extends CombineFn<KV<String, Integer>, BestMotifFinder.Motifs, KV<String, Integer>>{

	private static final long serialVersionUID = 5985853546163522609L;

	public static class Motifs implements Serializable {
		private static final long serialVersionUID = 3487855103983268976L;
		String motif = "";
		Integer distance = Integer.MAX_VALUE;
//		static AtomicInteger count = new AtomicInteger(0);

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Motifs other = (Motifs) obj;
			if (distance == null) {
				if (other.distance != null)
					return false;
			} else if (!distance.equals(other.distance))
				return false;
			if (motif == null) {
				if (other.motif != null)
					return false;
			} else if (!motif.equals(other.motif))
				return false;
			return true;
		}
	}

	@Override
	public Motifs createAccumulator() {
		// TODO Auto-generated method stub
//		System.out.println(Motifs.count.getAndIncrement());
		return new Motifs();
		
	}

	@Override
	public Motifs addInput(Motifs motif, KV<String, Integer> input) {
		// TODO Auto-generated method stub
		motif.motif= input.getKey();
		motif.distance = input.getValue();
		System.out.println("Motif: " + motif.motif +  " Distance : " + motif.distance);
		return motif;
	}

	@Override
	public Motifs mergeAccumulators(Iterable<Motifs> motifs) {
		Motifs returnMotif = createAccumulator();
		returnMotif.distance = Integer.MAX_VALUE;
		System.out.println("Currently in mergeAccumulators method");
		for(Motifs motif : motifs ) {
			if(motif.distance < returnMotif.distance) {
				returnMotif = motif;
			}
		}
		System.out.println("Best Motif" + " : " + returnMotif.motif);
		return returnMotif;
	}

	@Override
	public KV<String, Integer> extractOutput(Motifs bestMotif) {
		// TODO Auto-generated method stub
		System.out.println(bestMotif.motif + " --- " + bestMotif.distance);
		return KV.of(bestMotif.motif, bestMotif.distance);
	}
}
