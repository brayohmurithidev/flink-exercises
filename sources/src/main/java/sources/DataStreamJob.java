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

package sources;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.List;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		GET FROM ELEMENTS/ COLLECTIONS
//		DataStream<Integer> stream = env.fromElements(1,2,3,4,5);

//		FROM COLLECTIONS -Should be of same type
//		List<Tuple2<String, Integer>> data = ("Brian", "Alex", 1, 2);

//		DataStream<>

//		FROM FILE
		DataStream<String> fileStream = env.readTextFile("/Users/fazitech/work/Java/flink-exercises/sources/input.txt");

		//Manipulate data
		//Split lines into words and map each word to a tuple (word, 1)
		DataStream<Tuple2<String, Integer>> wordCounts = fileStream
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public void flatMap(String line, Collector<String> out) throws Exception {
						// Split the line into words
						for (String word : line.split(" ")) {
							out.collect(word.toLowerCase()); // Convert to lowercase for consistency
						}
					}
				})
				.map(new MapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> map(String word) throws Exception {
						return new Tuple2<>(word, 1); // Map each word to (word, 1)
					}
				});


		//Group words and sum their occurrences
		DataStream<Tuple2<String, Integer>> summedCounts = wordCounts.keyBy(value -> value.f0).sum(1);

		//print the results
		summedCounts.print();
		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
}
