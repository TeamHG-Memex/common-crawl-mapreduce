package com.hyperiongray.ccmr.s3wordcount;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class WordCountJob {
	private static final Logger logger = LoggerFactory.getLogger(WordCountJob.class);

	/*
	 * inputs 2) the keywords file
	 */
	public static void main(String[] args) throws Exception {

		CustomHttpConnector httpConnector = new CustomHttpConnector();

		try {

			logger.info("Hello");

			File file = new File(args[1]);
			FileUtils.deleteQuietly(file);

			JobConf conf = new JobConf();
			conf.setJarByClass(com.hyperiongray.ccmr.s3wordcount.WordCountOnlyMapper.class);
			conf.setJobName("HG");

			conf.setMapperClass(WordCountOnlyMapper.class);
			conf.setNumMapTasks(Integer.valueOf(args[3]));
			conf.setNumReduceTasks(0);
			// conf.setReducerClass(WordCountReducer.class);
			// conf.setNumReduceTasks(1);

			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			conf.setOutputKeyClass(NullWritable.class);
			conf.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));

			String keywordsFileContent;
			try {
				keywordsFileContent = FileUtils.readFileToString(new File(args[2]), "UTF-8");
				logger.info(keywordsFileContent);
			} catch (Exception e) {
				keywordsFileContent = httpConnector.getContent(args[2]);
			}

			//
			conf.set("keywordsFileContent", keywordsFileContent);

			if (args.length > 3)
				conf.setInt("sampleSize", Integer.valueOf(args[4]));

			// URI uri = new URI(args[2]);
			// DistributedCache.addCacheFile(uri, conf);

			// WeightedKeyword weightedKeyword = new WeightedKeyword(fileContent);
			// conf.set("weightKeywords", weightedKeyword.getDefinedWeightedWords());

			JobClient.runJob(conf);
		} catch (Exception e) {
			logger.error("job failed", e);
		}
	}
}
