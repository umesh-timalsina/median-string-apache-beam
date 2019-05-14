package edu.siu.cs425.medianstringapachebeam;

import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class BestMotifPardoFunction extends DoFn<KV<String, Iterable<Integer>>,KV<String,Integer>> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext c) {
		
		Integer total = 0;
		Iterable<Integer> result = c.element().getValue();
		
		for (Integer s : result) {
			total += s;
			
		}
		
		c.output(KV.of(c.element().getKey(), total));

	}

}
