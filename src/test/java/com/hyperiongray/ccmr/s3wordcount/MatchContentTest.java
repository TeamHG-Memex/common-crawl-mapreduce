package com.hyperiongray.ccmr.s3wordcount;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class MatchContentTest {

//	private WordCountOnlyMapper instance = new WordCountOnlyMapper();
//	private static final String path = "/home/tomas/Work/work/Proyecto/memex/git/common-crawl-mapreduce/data/keywords/keywords";

	private static final String path = "/home/tomas/Work/work/Proyecto/memex/git/common-crawl-mapreduce/data/keywords/explosive-keywords.csv";

	private com.hyperiongray.ccmr.s3wordcount.ContentMatcher instance ;

	private com.hyperiongray.ccmr.s3wordcount.CustomHttpConnector httpConnector = new com.hyperiongray.ccmr.s3wordcount.CustomHttpConnector();

	@Before
	public void before(){
		String keywordsFileContent;
		try {
			keywordsFileContent = FileUtils.readFileToString(new File(path), "UTF-8");
			instance = new com.hyperiongray.ccmr.s3wordcount.ContentMatcher(keywordsFileContent);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	@Test
	public void matchContentTest() {
		
		String content = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaagfeaaa bbbj gfe++aaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
				+ "bbbaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
		try {
			Map<String, Integer> matches = instance.matchContent(content);
			System.out.println(matches);
			int score = instance.score(matches);
			System.out.println(score);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void crawlingMatchContentTest() {
//		String url = "http://charlibella.rare-playmate.com/faq.php";
		String url = "http://charlibella.rare-playmate.com/faq-32431-bbbjtcim.php";
//		String url ="https://s3-us-west-2.amazonaws.com/darpa-memex/data/keywords/keywords.txt";
		
		String content = httpConnector.getContent(url);
		System.out.println(content);
		try {
			Map<String, Integer> matches = instance.matchContent(content);
			System.out.println(matches);
			int score = instance.score(matches);
			System.out.println(score);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
