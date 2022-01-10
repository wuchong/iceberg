/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * .
 */
public class KafkaOffsetsUtils {

	public static String kafkaOffsetsToString(Map<Integer, Long> offsets) {
		List<String> lists = new ArrayList<>();
		offsets.forEach((k,v) -> lists.add(k + ":"  + v));
		return String.join(",", lists);
	}

	public static Map<Integer, Long> stringToKafkaOffsets(String str) {
		if (str == null || str.length() == 0) {
			return new HashMap<>();
		}
		return Arrays.stream(str.split(","))
				.map(s -> s.split(":"))
				.collect(Collectors.toMap(s -> Integer.parseInt(s[0]), s -> Long.parseLong(s[1])));
	}

	public static void main(String[] args) {
		System.out.println(stringToKafkaOffsets(""));
	}
}
