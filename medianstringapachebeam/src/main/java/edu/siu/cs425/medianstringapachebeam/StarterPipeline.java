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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

  public static void main(String[] args) {
	  // Create a PipelineOptions object. This object lets us set various execution
	  // options for our pipeline, such as the runner you wish to use. This example
	  // will run with the DirectRunner by default, based on the class path configured
	  // in its dependencies.
	  PipelineOptions options = PipelineOptionsFactory.create();
	  Pipeline p = Pipeline.create(options);
	  
	  // Read from the local storage
	  p.apply(TextIO.read().from("/home/tumesh/median-string-apache-beam/medianstringapachebeam/src/main/resources/promoters_data_clean.txt"))
	  	.apply("Linecandidatefunction",ParDo.of(new LineCandidatePardoFunction()))
	  	.apply("Grouping Target Motifs for each line", GroupByKey.<String, Integer>create())
	  	.apply("Find Best", ParDo.of(new BestMotifPardoFunction()));
//	  	.apply(TextIO.write().to("/home/tumesh/median-string-apache-beam/medianstringapachebeam/src/main/resources/output"));
	    

	  p.run();

	  
  }
}
