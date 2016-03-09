package com.hyperiongray.ccmr.s3wordcount;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;


//java -cp target/common-crawler-0.1-SNAPSHOT-jar-with-dependencies.jar com.hyperiongray.s3wordcount.UrlScorer /home/tomas/Work/work/Proyecto/memex/git/memex-hackathon-1/common-crawl/data/keywords/keywords http://charlibella.rare-playmate.com/faq-32431-bbbjtcim.php
public class UrlScorer {

	private static final Logger logger = LoggerFactory.getLogger(UrlScorer.class);
	
	private CustomHttpConnector httpConnector = new CustomHttpConnector();
	private ContentMatcher contentMatcher;
	
	private void run(String keywordsPath, String url){
		
		String keywordsFileContent ="";
		try {
			keywordsFileContent = FileUtils.readFileToString(new File(keywordsPath), "UTF-8");
			logger.info(keywordsFileContent);
		} catch (Exception e) {
			keywordsFileContent = httpConnector.getContent(keywordsPath);
		}
		contentMatcher = new ContentMatcher(keywordsFileContent);

		
		String content = httpConnector.getContent(url);
		System.out.println(content);
		try {
			Map<String, Integer> matches = contentMatcher.matchContent(content);
			System.out.println(matches);
			int score = contentMatcher.score(matches);
			System.out.println(score);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		UrlScorer urlScorer = new UrlScorer();		
		String keywordsPath = args[0];
		String url = args[1];
		urlScorer.run(keywordsPath, url);
	}

}
