/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.siu.cs425.medianstringapachebeam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A starter example for writing Beam programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectRunner, just execute it
 * without any additional parameters from your favorite development environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 */

public class StarterPipeline {

	public interface MedianStringRunnerOptions extends PipelineOptions {

		/**
		 * By default, this example reads from a public dataset containing the text of
		 * King Lear. Set this option to choose a different input file or glob.
		 */
		@Description("Path of the file to read from")
		@Default.String("/home/utimalsina/workspace/medianstringapachebeam/src/main/resources/promoters_data_clean.txt")
		String getInputFile();

		void setInputFile(String value);

		/** Set this required option to specify where to write the output. */
		@Description("Path of the file to write to")
		@Required
		@Default.String("/home/utimalsina/workspace/medianstringapachebeam/src/main/resources/output.txt")
		String getOutput();

		void setOutput(String value);
	}

	private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

	public static void main(String[] args) {
		// Create a PipelineOptions object. This object lets us set various execution
		// options for our pipeline, such as the runner you wish to use. This example
		// will run with the DirectRunner by default, based on the class path configured
		// in its dependencies.
		MedianStringRunnerOptions options =
		        PipelineOptionsFactory.fromArgs(args).withValidation().as(MedianStringRunnerOptions.class);

		runMedianStringPipeline(options);
	}
	
	private static void runMedianStringPipeline(MedianStringRunnerOptions options) {
		Pipeline p = Pipeline.create(options);

		// Read from the local storage
		PCollection<KV<String, Integer>> combinedMotifKeys = p.apply(TextIO.read().from(options.getInputFile()))
				.apply("Linecandidatefunction", ParDo.of(new LineCandidatePardoFunction()))
				.apply("Find Consensus", Combine.perKey(new SumDistances()));

//	  	.apply("Find minimum", Min.doublesGlobally());
		PCollection<KV<String, Integer>> output = combinedMotifKeys.apply("Find Best", Combine
				.<KV<String, Integer>, KV<String, Integer>>globally(new BestMotifFinderTry2()).withoutDefaults());
		
		output.apply(MapElements.via(new FormatAsTextFn()))
        .apply("WriteCounts", TextIO.write().to(options.getOutput()));
		

		p.run().waitUntilFinish();

	}
	
	public static class FormatAsTextFn extends SimpleFunction<KV<String, Integer>, String> {
	    @Override
	    public String apply(KV<String, Integer> input) {
	      return input.getKey() + ": " + input.getValue();
	    }
	  }
}
