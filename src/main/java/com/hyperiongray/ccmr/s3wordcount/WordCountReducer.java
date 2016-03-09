package com.hyperiongray.ccmr.s3wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

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

public class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Map<Text, IntWritable>> {

	public void reduce(Text url, Iterator<IntWritable> scores, OutputCollector<Text, Map<Text, IntWritable>> results, Reporter reporter) throws IOException {

		String domainName;
		try {
			domainName = this.getDomainName(url.toString());
			Map<Text, IntWritable> map = Maps.newHashMap();
			int acum = 0;
			
//			Integer score = Integer.valueOf(scores.get("score").toString());
			while (scores.hasNext()) {
				acum += scores.next().get();
			}
//			results.collect(new Text(domainName), new IntWritable(acum));
			
			map.put(new Text(url), new IntWritable(acum));
			results.collect(new Text(domainName), map);
			
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	public String getDomainName(String url) throws URISyntaxException {
		URI uri = new URI(url);
		String domain = uri.getHost();
		return domain.startsWith("www.") ? domain.substring(4) : domain;
	}

}
