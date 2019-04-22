package edu.siu.cs425.medianstringapachebeam;

import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class BestMotifPardoFunction extends DoFn<PCollection<KV<String, Iterable<Integer>>>, String>{


	private static final long serialVersionUID = 1L;
	
	public void processElement(@Element PCollection<KV<String, Iterable<Integer>>> key, OutputReceiver<String> value) {
//		System.out.println(key.get
	}
}
