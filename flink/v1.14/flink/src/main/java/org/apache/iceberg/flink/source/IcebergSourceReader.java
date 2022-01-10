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

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;

import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * .
 */
public class IcebergSourceReader implements SourceReader<RowData, IcebergFileSplit> {

	private final SourceReaderContext context;
	private final FileIO io;
	private final EncryptionManager encryption;
	private final RowDataFileScanTaskReader rowDataReader;

	private transient DataIterator<RowData> iterator;

	private CompletableFuture<Void> availability;

	private boolean noMoreSplits;

	private final Queue<IcebergFileSplit> remainingSplits;
	@Nullable private IcebergFileSplit currentSplit;

	public IcebergSourceReader(SourceReaderContext context, Schema tableSchema,  FileIO io, EncryptionManager encryption, ScanContext scanContext) {
		this.context = context;
		this.io = io;
		this.encryption = encryption;
		this.availability = new CompletableFuture<>();
		this.remainingSplits =  new ArrayDeque<>();
		this.rowDataReader = new RowDataFileScanTaskReader(tableSchema,
				scanContext.project(), scanContext.nameMapping(), scanContext.caseSensitive());
	}

	@Override
	public void start() {
		// request a split if we don't have one
		if (remainingSplits.isEmpty()) {
			context.sendSplitRequest();
		}
	}

	@Override
	public InputStatus pollNext(ReaderOutput<RowData> output) {
		if (iterator != null) {
			if (iterator.hasNext()) {
				output.collect(iterator.next());
				return InputStatus.MORE_AVAILABLE;
			} else {
				finishSplit();
			}
		}

		return tryMoveToNextSplit();
	}

	private void finishSplit() {
		if (iterator != null) {
			try {
				iterator.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		iterator = null;
		currentSplit = null;

		// request another split if no other is left
		// we do this only here in the finishSplit part to avoid requesting a split
		// whenever the reader is polled and doesn't currently have a split
		if (remainingSplits.isEmpty() && !noMoreSplits) {
			context.sendSplitRequest();
		}
	}

	private InputStatus tryMoveToNextSplit() {
		currentSplit = remainingSplits.poll();
		if (currentSplit != null) {
			iterator = new DataIterator<>(rowDataReader, currentSplit.getTask(), io, encryption);
			return InputStatus.MORE_AVAILABLE;
		} else if (noMoreSplits) {
			return InputStatus.END_OF_INPUT;
		} else {
			// ensure we are not called in a loop by resetting the availability future
			if (availability.isDone()) {
				availability = new CompletableFuture<>();
			}

			return InputStatus.NOTHING_AVAILABLE;
		}
	}

	@Override
	public List<IcebergFileSplit> snapshotState(long checkpointId) {
		if (currentSplit == null && remainingSplits.isEmpty()) {
			return Collections.emptyList();
		}

		final ArrayList<IcebergFileSplit> allSplits = new ArrayList<>(remainingSplits);
		allSplits.add(currentSplit);
		return allSplits;
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
		return availability;
	}

	@Override
	public void addSplits(List<IcebergFileSplit> splits) {
		remainingSplits.addAll(splits);
		// set availability so that pollNext is actually called
		availability.complete(null);
	}

	@Override
	public void notifyNoMoreSplits() {
		noMoreSplits = true;
		// set availability so that pollNext is actually called
		availability.complete(null);
	}

	@Override
	public void close() throws Exception {

	}
}
