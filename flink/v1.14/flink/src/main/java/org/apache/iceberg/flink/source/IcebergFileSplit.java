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

import org.apache.flink.api.connector.source.SourceSplit;

import org.apache.iceberg.CombinedScanTask;

import java.io.Serializable;

/**
 * .
 */
public class IcebergFileSplit implements SourceSplit, Serializable {

	private final int splitNumber;
	private final CombinedScanTask task;

	public IcebergFileSplit(int splitNumber, CombinedScanTask task) {
		this.splitNumber = splitNumber;
		this.task = task;
	}

	public int getSplitNumber() {
		return splitNumber;
	}

	public CombinedScanTask getTask() {
		return task;
	}

	public FlinkInputSplit toFlinkInputSplit() {
		return new FlinkInputSplit(splitNumber, task);
	}

	@Override
	public String splitId() {
		return splitNumber + "";
	}
}
