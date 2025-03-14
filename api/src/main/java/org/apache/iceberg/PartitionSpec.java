/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;

/**
 * Represents how to produce partition data for a table.
 * <p>
 * Partition data is produced by transforming columns in a table. Each column transform is
 * represented by a named {@link PartitionField}.
 */
public class PartitionSpec implements Serializable {
  // IDs for partition fields start at 1000
  private static final int PARTITION_DATA_ID_START = 1000;

  private final Schema schema;

  // this is ordered so that DataFile has a consistent schema
  private final int specId;
  private final PartitionField[] fields;
  private transient volatile ListMultimap<Integer, PartitionField> fieldsBySourceId = null;
  private transient volatile Class<?>[] lazyJavaClasses = null;
  private transient volatile List<PartitionField> fieldList = null;
  private final int lastAssignedFieldId;

  private PartitionSpec(Schema schema, int specId, List<PartitionField> fields, int lastAssignedFieldId) {
    this.schema = schema;
    this.specId = specId;
    this.fields = new PartitionField[fields.size()];
    for (int i = 0; i < this.fields.length; i += 1) {
      this.fields[i] = fields.get(i);
    }
    this.lastAssignedFieldId = lastAssignedFieldId;
  }

  /**
   * Returns the {@link Schema} for this spec.
   */
  public Schema schema() {
    return schema;
  }

  /**
   * Returns the ID of this spec.
   */
  public int specId() {
    return specId;
  }

  /**
   * Returns the list of {@link PartitionField partition fields} for this spec.
   */
  public List<PartitionField> fields() {
    return lazyFieldList();
  }

  public boolean isUnpartitioned() {
    return fields.length < 1;
  }

  int lastAssignedFieldId() {
    return lastAssignedFieldId;
  }

  /**
   * Returns the {@link PartitionField field} that partitions the given source field
   *
   * @param fieldId a field id from the source schema
   * @return the {@link PartitionField field} that partitions the given source field
   */
  public List<PartitionField> getFieldsBySourceId(int fieldId) {
    return lazyFieldsBySourceId().get(fieldId);
  }

  /**
   * Returns a {@link StructType} for partition data defined by this spec.
   */
  public StructType partitionType() {
    List<Types.NestedField> structFields = Lists.newArrayListWithExpectedSize(fields.length);

    for (int i = 0; i < fields.length; i += 1) {
      PartitionField field = fields[i];
      Type sourceType = schema.findType(field.sourceId());
      Type resultType = field.transform().getResultType(sourceType);
      structFields.add(
          Types.NestedField.optional(field.fieldId(), field.name(), resultType));
    }

    return Types.StructType.of(structFields);
  }

  public Class<?>[] javaClasses() {
    if (lazyJavaClasses == null) {
      synchronized (this) {
        if (lazyJavaClasses == null) {
          Class<?>[] classes = new Class<?>[fields.length];
          for (int i = 0; i < fields.length; i += 1) {
            PartitionField field = fields[i];
            if (field.transform() instanceof UnknownTransform) {
              classes[i] = Object.class;
            } else {
              Type sourceType = schema.findType(field.sourceId());
              Type result = field.transform().getResultType(sourceType);
              classes[i] = result.typeId().javaClass();
            }
          }

          this.lazyJavaClasses = classes;
        }
      }
    }

    return lazyJavaClasses;
  }

  @SuppressWarnings("unchecked")
  private <T> T get(StructLike data, int pos, Class<?> javaClass) {
    return data.get(pos, (Class<T>) javaClass);
  }

  private String escape(String string) {
    try {
      return URLEncoder.encode(string, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public String partitionToPath(StructLike data) {
    StringBuilder sb = new StringBuilder();
    Class<?>[] javaClasses = javaClasses();
    for (int i = 0; i < javaClasses.length; i += 1) {
      PartitionField field = fields[i];
      String valueString = field.transform().toHumanString(get(data, i, javaClasses[i]));

      if (i > 0) {
        sb.append("/");
      }
      sb.append(field.name()).append("=").append(escape(valueString));
    }
    return sb.toString();
  }

  /**
   * Returns true if this spec is equivalent to the other, with partition field ids ignored.
   * That is, if both specs have the same number of fields, field order, field name, source columns, and transforms.
   *
   * @param other another PartitionSpec
   * @return true if the specs have the same fields, source columns, and transforms.
   */
  public boolean compatibleWith(PartitionSpec other) {
    if (equals(other)) {
      return true;
    }

    if (fields.length != other.fields.length) {
      return false;
    }

    for (int i = 0; i < fields.length; i += 1) {
      PartitionField thisField = fields[i];
      PartitionField thatField = other.fields[i];
      if (thisField.sourceId() != thatField.sourceId() ||
          !thisField.transform().toString().equals(thatField.transform().toString()) ||
          !thisField.name().equals(thatField.name())) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof PartitionSpec)) {
      return false;
    }

    PartitionSpec that = (PartitionSpec) other;
    if (this.specId != that.specId) {
      return false;
    }
    return Arrays.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return 31 * Integer.hashCode(specId) + Arrays.hashCode(fields);
  }

  private List<PartitionField> lazyFieldList() {
    if (fieldList == null) {
      synchronized (this) {
        if (fieldList == null) {
          this.fieldList = ImmutableList.copyOf(fields);
        }
      }
    }
    return fieldList;
  }

  private ListMultimap<Integer, PartitionField> lazyFieldsBySourceId() {
    if (fieldsBySourceId == null) {
      synchronized (this) {
        if (fieldsBySourceId == null) {
          ListMultimap<Integer, PartitionField> multiMap = Multimaps
              .newListMultimap(Maps.newHashMap(), () -> Lists.newArrayListWithCapacity(fields.length));
          for (PartitionField field : fields) {
            multiMap.put(field.sourceId(), field);
          }
          this.fieldsBySourceId = multiMap;
        }
      }
    }

    return fieldsBySourceId;
  }

  /**
   * Returns the source field ids for identity partitions.
   *
   * @return a set of source ids for the identity partitions.
   */
  public Set<Integer> identitySourceIds() {
    Set<Integer> sourceIds = Sets.newHashSet();
    for (PartitionField field : fields()) {
      if ("identity".equals(field.transform().toString())) {
        sourceIds.add(field.sourceId());
      }
    }

    return sourceIds;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (PartitionField field : fields) {
      sb.append("\n");
      sb.append("  ").append(field);
    }
    if (fields.length > 0) {
      sb.append("\n");
    }
    sb.append("]");
    return sb.toString();
  }

  private static final PartitionSpec UNPARTITIONED_SPEC =
      new PartitionSpec(new Schema(), 0, ImmutableList.of(), PARTITION_DATA_ID_START - 1);

  /**
   * Returns a spec for unpartitioned tables.
   *
   * @return a partition spec with no partitions
   */
  public static PartitionSpec unpartitioned() {
    return UNPARTITIONED_SPEC;
  }

  /**
   * Creates a new {@link Builder partition spec builder} for the given {@link Schema}.
   *
   * @param schema a schema
   * @return a partition spec builder for the given schema
   */
  public static Builder builderFor(Schema schema) {
    return new Builder(schema);
  }

  /**
   * Used to create valid {@link PartitionSpec partition specs}.
   * <p>
   * Call {@link #builderFor(Schema)} to create a new builder.
   */
  public static class Builder {
    private final Schema schema;
    private final List<PartitionField> fields = Lists.newArrayList();
    private final Set<String> partitionNames = Sets.newHashSet();
    private Map<Map.Entry<Integer, String>, PartitionField> dedupFields = Maps.newHashMap();
    private int specId = 0;
    private final AtomicInteger lastAssignedFieldId = new AtomicInteger(PARTITION_DATA_ID_START - 1);
    // check if there are conflicts between partition and schema field name
    private boolean checkConflicts = true;

    private Builder(Schema schema) {
      this.schema = schema;
    }

    private int nextFieldId() {
      return lastAssignedFieldId.incrementAndGet();
    }

    private void checkAndAddPartitionName(String name) {
      checkAndAddPartitionName(name, null);
    }

    Builder checkConflicts(boolean check) {
      checkConflicts = check;
      return this;
    }

    private void checkAndAddPartitionName(String name, Integer sourceColumnId) {
      Types.NestedField schemaField = schema.findField(name);
      if (checkConflicts) {
        if (sourceColumnId != null) {
          // for identity transform case we allow conflicts between partition and schema field name as
          //   long as they are sourced from the same schema field
          Preconditions.checkArgument(schemaField == null || schemaField.fieldId() == sourceColumnId,
              "Cannot create identity partition sourced from different field in schema: %s", name);
        } else {
          // for all other transforms we don't allow conflicts between partition name and schema field name
          Preconditions.checkArgument(schemaField == null,
              "Cannot create partition from name that exists in schema: %s", name);
        }
      }
      Preconditions.checkArgument(name != null && !name.isEmpty(),
          "Cannot use empty or null partition name: %s", name);
      Preconditions.checkArgument(!partitionNames.contains(name),
          "Cannot use partition name more than once: %s", name);
      partitionNames.add(name);
    }

    private void checkForRedundantPartitions(PartitionField field) {
      Map.Entry<Integer, String> dedupKey = new AbstractMap.SimpleEntry<>(
          field.sourceId(), field.transform().dedupName());
      PartitionField partitionField = dedupFields.get(dedupKey);
      Preconditions.checkArgument(partitionField == null,
          "Cannot add redundant partition: %s conflicts with %s", partitionField, field);
      dedupFields.put(dedupKey, field);
    }

    public Builder withSpecId(int newSpecId) {
      this.specId = newSpecId;
      return this;
    }

    private Types.NestedField findSourceColumn(String sourceName) {
      Types.NestedField sourceColumn = schema.findField(sourceName);
      Preconditions.checkArgument(sourceColumn != null, "Cannot find source column: %s", sourceName);
      return sourceColumn;
    }

    Builder identity(String sourceName, String targetName) {
      Types.NestedField sourceColumn = findSourceColumn(sourceName);
      checkAndAddPartitionName(targetName, sourceColumn.fieldId());
      PartitionField field = new PartitionField(
          sourceColumn.fieldId(), nextFieldId(), targetName, Transforms.identity(sourceColumn.type()));
      checkForRedundantPartitions(field);
      fields.add(field);
      return this;
    }

    public Builder identity(String sourceName) {
      return identity(sourceName, sourceName);
    }

    public Builder year(String sourceName, String targetName) {
      checkAndAddPartitionName(targetName);
      Types.NestedField sourceColumn = findSourceColumn(sourceName);
      PartitionField field = new PartitionField(
          sourceColumn.fieldId(), nextFieldId(), targetName, Transforms.year(sourceColumn.type()));
      checkForRedundantPartitions(field);
      fields.add(field);
      return this;
    }

    public Builder year(String sourceName) {
      return year(sourceName, sourceName + "_year");
    }

    public Builder month(String sourceName, String targetName) {
      checkAndAddPartitionName(targetName);
      Types.NestedField sourceColumn = findSourceColumn(sourceName);
      PartitionField field = new PartitionField(
          sourceColumn.fieldId(), nextFieldId(), targetName, Transforms.month(sourceColumn.type()));
      checkForRedundantPartitions(field);
      fields.add(field);
      return this;
    }

    public Builder month(String sourceName) {
      return month(sourceName, sourceName + "_month");
    }

    public Builder day(String sourceName, String targetName) {
      checkAndAddPartitionName(targetName);
      Types.NestedField sourceColumn = findSourceColumn(sourceName);
      PartitionField field = new PartitionField(
          sourceColumn.fieldId(), nextFieldId(), targetName, Transforms.day(sourceColumn.type()));
      checkForRedundantPartitions(field);
      fields.add(field);
      return this;
    }

    public Builder day(String sourceName) {
      return day(sourceName, sourceName + "_day");
    }

    public Builder hour(String sourceName, String targetName) {
      checkAndAddPartitionName(targetName);
      Types.NestedField sourceColumn = findSourceColumn(sourceName);
      PartitionField field = new PartitionField(
          sourceColumn.fieldId(), nextFieldId(), targetName, Transforms.hour(sourceColumn.type()));
      checkForRedundantPartitions(field);
      fields.add(field);
      return this;
    }

    public Builder hour(String sourceName) {
      return hour(sourceName, sourceName + "_hour");
    }

    public Builder bucket(String sourceName, int numBuckets, String targetName) {
      checkAndAddPartitionName(targetName);
      Types.NestedField sourceColumn = findSourceColumn(sourceName);
      fields.add(new PartitionField(
          sourceColumn.fieldId(), nextFieldId(), targetName, Transforms.bucket(sourceColumn.type(), numBuckets)));
      return this;
    }

    public Builder bucket(String sourceName, int numBuckets) {
      return bucket(sourceName, numBuckets, sourceName + "_bucket");
    }

    public Builder truncate(String sourceName, int width, String targetName) {
      checkAndAddPartitionName(targetName);
      Types.NestedField sourceColumn = findSourceColumn(sourceName);
      fields.add(new PartitionField(
          sourceColumn.fieldId(), nextFieldId(), targetName, Transforms.truncate(sourceColumn.type(), width)));
      return this;
    }

    public Builder truncate(String sourceName, int width) {
      return truncate(sourceName, width, sourceName + "_trunc");
    }

    public Builder alwaysNull(String sourceName, String targetName) {
      Types.NestedField sourceColumn = findSourceColumn(sourceName);
      checkAndAddPartitionName(targetName, sourceColumn.fieldId()); // can duplicate a source column name
      fields.add(new PartitionField(sourceColumn.fieldId(), nextFieldId(), targetName, Transforms.alwaysNull()));
      return this;
    }

    public Builder alwaysNull(String sourceName) {
      return alwaysNull(sourceName, sourceName + "_null");
    }

    // add a partition field with an auto-increment partition field id starting from PARTITION_DATA_ID_START
    Builder add(int sourceId, String name, String transform) {
      return add(sourceId, nextFieldId(), name, transform);
    }

    Builder add(int sourceId, int fieldId, String name, String transform) {
      Types.NestedField column = schema.findField(sourceId);
      Preconditions.checkNotNull(column, "Cannot find source column: %s", sourceId);
      return add(sourceId, fieldId, name, Transforms.fromString(column.type(), transform));
    }

    Builder add(int sourceId, int fieldId, String name, Transform<?, ?> transform) {
      checkAndAddPartitionName(name, sourceId);
      fields.add(new PartitionField(sourceId, fieldId, name, transform));
      lastAssignedFieldId.getAndAccumulate(fieldId, Math::max);
      return this;
    }

    public PartitionSpec build() {
      PartitionSpec spec = buildUnchecked();
      checkCompatibility(spec, schema);
      return spec;
    }

    PartitionSpec buildUnchecked() {
      return new PartitionSpec(schema, specId, fields, lastAssignedFieldId.get());
    }
  }

  static void checkCompatibility(PartitionSpec spec, Schema schema) {
    for (PartitionField field : spec.fields) {
      Type sourceType = schema.findType(field.sourceId());
      ValidationException.check(sourceType != null,
          "Cannot find source column for partition field: %s", field);
      ValidationException.check(sourceType.isPrimitiveType(),
          "Cannot partition by non-primitive source field: %s", sourceType);
      ValidationException.check(
          field.transform().canTransform(sourceType),
          "Invalid source type %s for transform: %s",
          sourceType, field.transform());
    }
  }

  static boolean hasSequentialIds(PartitionSpec spec) {
    for (int i = 0; i < spec.fields.length; i += 1) {
      if (spec.fields[i].fieldId() != PARTITION_DATA_ID_START + i) {
        return false;
      }
    }
    return true;
  }
}
