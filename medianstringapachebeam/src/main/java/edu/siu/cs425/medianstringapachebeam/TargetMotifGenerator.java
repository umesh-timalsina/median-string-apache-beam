package edu.siu.cs425.medianstringapachebeam;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TargetMotifGenerator {

	public static final char[] availableChars = { 'a', 'c', 'g', 't' };
	public static List<String> allPossibleSequences = new LinkedList<>();
	private static final Logger logger = LoggerFactory.getLogger(TargetMotifGenerator.class);

	// Provide a static method for all sequences and store it in a variable here
	/*
	 * Generate all possible combinations for target motifs, Given the length of the
	 * target motif
	 */
	public static List<String> generateTargetMotifs(int length) {
		allPossibleSequences.clear();
		generateAllCombinations(availableChars, "", availableChars.length, length);
//		logger.info("Generated {} possible combinations for target Length {}", allPossibleSequences.size(), length);
		return allPossibleSequences;
		
	}

	private static void generateAllCombinations(char[] set, String prefix, int n, int k) {
		if (k == 0) {
			allPossibleSequences.add(prefix);
//			System.out.println(allPossibleSequences.get(allPossibleSequences.size()-1));  // Sanity Print
			return;
		}
		for (int i = 0; i < n; i++) {
			String newPrefix = prefix + set[i];
			generateAllCombinations(set, newPrefix, n, k - 1);
		}
	}
}
