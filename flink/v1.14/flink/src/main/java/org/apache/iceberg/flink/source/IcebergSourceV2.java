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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.InstantiationUtil;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.FileIO;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;

/**
 * .
 */
public class IcebergSourceV2 implements Source<RowData, IcebergFileSplit, Collection<IcebergFileSplit>> {

	private final Map<Integer, Long> kafkaOffsets;
	private final TableLoader tableLoader;
	private final FileIO io;
	private final EncryptionManager encryption;
	private final ScanContext scanContext;
	private final Schema tableSchema;

	public IcebergSourceV2(Map<Integer, Long> kafkaOffsets, TableLoader tableLoader, FileIO io, EncryptionManager encryption, ScanContext scanContext, Schema tableSchema) {
		this.kafkaOffsets = kafkaOffsets;
		this.tableLoader = tableLoader;
		this.io = io;
		this.encryption = encryption;
		this.scanContext = scanContext;
		this.tableSchema = tableSchema;
	}

	public Map<Integer, Long> kafkaOffsets() {
		return kafkaOffsets;
	}

	@Override
	public Boundedness getBoundedness() {
		return Boundedness.BOUNDED;
	}

	@Override
	public SourceReader<RowData, IcebergFileSplit> createReader(SourceReaderContext readerContext) throws Exception {
		return new IcebergSourceReader(readerContext, tableSchema, io, encryption, scanContext);
	}

	@Override
	public SplitEnumerator<IcebergFileSplit, Collection<IcebergFileSplit>> createEnumerator(SplitEnumeratorContext<IcebergFileSplit> enumContext) throws Exception {
		// Called in Job manager, so it is OK to load table from catalog.
		Queue<IcebergFileSplit> remainingSplits = new ArrayDeque<>();
		tableLoader.open();
		try (TableLoader loader = tableLoader) {
			Table table = loader.loadTable();
			FlinkInputSplit[] inputSplits = FlinkSplitGenerator.createInputSplits(table, scanContext);
			for (FlinkInputSplit split : inputSplits) {
				remainingSplits.add(new IcebergFileSplit(split.getSplitNumber(), split.getTask()));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return new IcebergSplitEnumerator(enumContext, remainingSplits);
	}

	@Override
	public SplitEnumerator<IcebergFileSplit, Collection<IcebergFileSplit>> restoreEnumerator(SplitEnumeratorContext<IcebergFileSplit> enumContext, Collection<IcebergFileSplit> checkpoint) throws Exception {
		return new IcebergSplitEnumerator(enumContext, new ArrayDeque<>(checkpoint));
	}

	@Override
	public SimpleVersionedSerializer<IcebergFileSplit> getSplitSerializer() {
		return new SimpleVersionedSerializer<IcebergFileSplit>() {
			@Override
			public int getVersion() {
				return 0;
			}

			@Override
			public byte[] serialize(IcebergFileSplit obj) throws IOException {
				return InstantiationUtil.serializeObject(obj);
			}

			@Override
			public IcebergFileSplit deserialize(int version, byte[] serialized) throws IOException {
				try {
					return InstantiationUtil.deserializeObject(serialized, Thread.currentThread().getContextClassLoader());
				} catch (ClassNotFoundException e) {
					throw new RuntimeException(e);
				}
			}
		};
	}

	@Override
	public SimpleVersionedSerializer<Collection<IcebergFileSplit>> getEnumeratorCheckpointSerializer() {
		return new SimpleVersionedSerializer<Collection<IcebergFileSplit>>() {
			@Override
			public int getVersion() {
				return 0;
			}

			@Override
			public byte[] serialize(Collection<IcebergFileSplit> obj) throws IOException {
				return InstantiationUtil.serializeObject(
						obj.toArray(new IcebergFileSplit[obj.size()]));
			}

			@Override
			public Collection<IcebergFileSplit> deserialize(int version, byte[] serialized) throws IOException {
				IcebergFileSplit[] splitArray;
				try {
					splitArray =
							InstantiationUtil.deserializeObject(
									serialized, getClass().getClassLoader());
				} catch (ClassNotFoundException e) {
					throw new IOException("Failed to deserialize the source split.");
				}
				return new ArrayList<>(Arrays.asList(splitArray));
			}
		};
	}
}
