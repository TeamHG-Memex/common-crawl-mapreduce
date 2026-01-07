package com.hyperiongray.ccmr.s3wordcount;

import org.apache.hadoop.thirdparty.guava.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ContentMatcher {

	private static final Logger logger = LoggerFactory.getLogger(ContentMatcher.class);
	private static final Pattern pattern = Pattern.compile("<title>(.+?)</title>", Pattern.CASE_INSENSITIVE);
	private com.hyperiongray.ccmr.s3wordcount.WeightedKeyword weightedKeyword;

	public ContentMatcher(String keywordsfileContent) {
		weightedKeyword = new com.hyperiongray.ccmr.s3wordcount.WeightedKeyword(keywordsfileContent);
		logger.info("weights:" + weightedKeyword.getDefinedWeightedWords());
		logger.info("weights:" + weightedKeyword.getKeywordPattern());
	}

	public Map<String, Integer> matchContent(String content) throws IOException {
		Map<String, Integer> matches = Maps.newHashMap();
		if (!content.isEmpty() && content.length() > 50) {
			for (Entry<String, Pattern> entry : weightedKeyword.getKeywordPattern().entrySet()) {
				Matcher matcher = entry.getValue().matcher(content);
				if (matcher.find())
					matches.put(entry.getKey(), 1);
			}
		}
		return matches;
	}

	// public for testing purposes
	public int score(Map<String, Integer> matches) throws IOException {
		int score = 1;
		for (Entry<String, Integer> entry : matches.entrySet()) {
			score *= weightedKeyword.getDefinedWeightedWords().get(entry.getKey()) * entry.getValue();
		}
		return score;
	}

	public String getTitle(String content) {
		Matcher matcher = pattern.matcher(content);
		String result;
		if (matcher.find()) {
			result = matcher.group(1);
		} else {
			result = "";
		}
		return result;
	}

}
