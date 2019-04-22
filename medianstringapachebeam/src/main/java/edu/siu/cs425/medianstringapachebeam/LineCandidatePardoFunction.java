package edu.siu.cs425.medianstringapachebeam;

import java.util.List;

import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineCandidatePardoFunction extends DoFn<String, KV<String, Integer>> {

	private static final long serialVersionUID = -256454516646536L;
	private static final Logger logger = LoggerFactory.getLogger(LineCandidatePardoFunction.class);
	private static final List<String> allPossibleSequences = TargetMotifGenerator.generateTargetMotifs(8);
	private static int count=0;
	@ProcessElement
	public void processElement(@Element String line, OutputReceiver<KV<String, Integer>> out) {
		int TARGET_LENGTH = 8;
		count++;
		for (String targetMotif : TargetMotifGenerator.allPossibleSequences) {
			String bestMatch = line.substring(0, TARGET_LENGTH);
			int distance = 0;
			int bestDistance = TARGET_LENGTH + 1; // Why not inf?

			int currIndex = 0;
			int tmpCurrIndex = 0;
			for (int startIndex = 0; startIndex < (line.length() - TARGET_LENGTH); startIndex++) {
				currIndex = 0;
				distance = 0;
				for (char single : targetMotif.toCharArray()) {
					if (single != line.charAt(startIndex + currIndex++)) {
						distance++;
					}
				}

				if (distance < bestDistance) {
					bestDistance = distance;
					tmpCurrIndex = startIndex;
				}
			}
//			logger.info("line: " + count + "-" + targetMotif + "-" + bestDistance);
			out.output(KV.of(targetMotif, bestDistance));
		}
		

	}
}
