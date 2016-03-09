package com.hyperiongray.ccmr.s3wordcount;

import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.util.Map;
import java.util.regex.Pattern;

public class WeightedKeyword {

	private static Map<String, Integer> definedWeightedWords = Maps.newHashMap();
	Map<String, Pattern> keywordPattern = Maps.newHashMap();

	public WeightedKeyword(String filecontent) {
		String[] lines = filecontent.split("\n");
		definedWeightedWords = Maps.newHashMap();
		for (String line : lines) {
			String[] split = line.split(",");
			String key = split[0].trim();
			definedWeightedWords.put(key, Integer.valueOf(split[1]));
			Pattern pattern = Pattern.compile("\\b" + Pattern.quote(key) + "\\b", Pattern.CASE_INSENSITIVE);
			keywordPattern.put(key, pattern);
		}
//		for (String keyword : getDefinedWeightedWords().keySet()) {
//			String escapedKeyword = Pattern.quote(keyword);
//			Pattern pattern = Pattern.compile("\\b" + escapedKeyword + "\\b", Pattern.CASE_INSENSITIVE);
//			keywordPattern.put(keyword, pattern);
//		}

	}

	public Map<String, Integer> getDefinedWeightedWords(){
		return definedWeightedWords;
	}

	public Map<String, Pattern> getKeywordPattern(){
		return keywordPattern;
	}
}
