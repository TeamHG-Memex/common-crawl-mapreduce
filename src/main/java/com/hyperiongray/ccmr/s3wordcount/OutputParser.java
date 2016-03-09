package com.hyperiongray.ccmr.s3wordcount;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class OutputParser {

	private static final String SEPARTOR = "|";

	public String parse(String title, String url, String crawledDate,Integer score, Map<String, Integer> matches) {
		StringBuilder builder = new StringBuilder();
		try {
			builder.append(score).append(SEPARTOR);
			builder.append(title).append(SEPARTOR);
			builder.append(getDomainName(url)).append(SEPARTOR);
			builder.append(crawledDate).append(SEPARTOR);
			builder.append(url).append(SEPARTOR);
			builder.append(matches).append(SEPARTOR);
		} catch (Exception e) {
			System.out.println(e);
		}
		return builder.toString();
	}

	public String getDomainName(String url) throws URISyntaxException {
		URI uri = new URI(url);
		String domain = uri.getHost();
		return domain.startsWith("www.") ? domain.substring(4) : domain;
	}

}
