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

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/**
 * .
 */
public class IcebergSplitEnumerator implements SplitEnumerator<IcebergFileSplit, Collection<IcebergFileSplit>> {

	private final SplitEnumeratorContext<IcebergFileSplit> context;
	private final Queue<IcebergFileSplit> remainingSplits;

	public IcebergSplitEnumerator(SplitEnumeratorContext<IcebergFileSplit> enumContext, Queue<IcebergFileSplit> remainingSplits) {
		this.context = enumContext;
		this.remainingSplits = new ArrayDeque<>(remainingSplits);
	}

	@Override
	public void start() {
	}

	@Override
	public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
		final IcebergFileSplit nextSplit = remainingSplits.poll();
		if (nextSplit != null) {
			context.assignSplit(nextSplit, subtaskId);
		} else {
			context.signalNoMoreSplits(subtaskId);
		}
	}

	@Override
	public void addSplitsBack(List<IcebergFileSplit> splits, int subtaskId) {
		remainingSplits.addAll(splits);
	}

	@Override
	public void addReader(int subtaskId) {

	}

	@Override
	public Collection<IcebergFileSplit> snapshotState(long checkpointId) throws Exception {
		return remainingSplits;
	}

	@Override
	public void close() throws IOException {

	}
}
