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

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class BaseStreamingUpdate extends MergingSnapshotProducer<StreamingUpdate>
    implements StreamingUpdate {
  private final List<Batch> batches = Lists.newArrayList();

  private boolean requiresApply = true;

  BaseStreamingUpdate(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  protected BaseStreamingUpdate self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.OVERWRITE;
  }

  @Override
  protected long nextSnapshotSequenceNumber(TableMetadata base) {
    if (batches.isEmpty()) {
      return super.nextSnapshotSequenceNumber(base);
    }
    // Each batch will advance the data sequence number by one, so we should advance the snapshot by
    // the same amount.
    // Otherwise, we will end up with data files with a sequence number larger than the snapshot
    // sequence number.
    // The validate method ensures we can use `.size()` here since it validates that there are no
    // gaps and that we start at 0.
    return super.nextSnapshotSequenceNumber(base) + batches.size() - 1;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base, Snapshot snapshot) {
    if (requiresApply && !batches.isEmpty()) {
      long startingSequenceNumber = base.nextSequenceNumber();
      batches.sort(Comparator.comparingInt(o -> o.ordinal));
      for (Batch batch : batches) {
        long dataSequenceNumber = startingSequenceNumber + batch.ordinal;
        batch.newDataFiles.forEach(f -> add(f, dataSequenceNumber));
        batch.newDeleteFiles.forEach(f -> add(f, dataSequenceNumber));
      }
      requiresApply = false;
    }
    return super.apply(base, snapshot);
  }

  @Override
  public StreamingUpdate addFile(DataFile dataFile, int batchOrdinal) {
    getBatch(batchOrdinal).add(dataFile);
    return this;
  }

  @Override
  public StreamingUpdate addFile(DeleteFile deleteFile, int batchOrdinal) {
    getBatch(batchOrdinal).add(deleteFile);
    return this;
  }

  private Batch getBatch(int batchOrdinal) {
    Batch batch;
    if (batches.size() - 1 < batchOrdinal) {
      if (batchOrdinal > batches.size()) {
        throw new IllegalArgumentException("Batches must be added in order");
      }
      batch = new Batch(batchOrdinal);
      batches.add(batch);
    } else {
      batch = batches.get(batchOrdinal);
    }
    return batch;
  }

  @Override
  public BaseStreamingUpdate toBranch(String branch) {
    targetBranch(branch);
    return this;
  }

  @Override
  protected void cleanUncommitted(Set<ManifestFile> committed) {
    // This is called when the commit fails and the caches are cleared, reset the state here so
    // calling apply again will re-add the files
    requiresApply = true;
    super.cleanUncommitted(committed);
  }

  @Override
  protected void validate(TableMetadata currentMetadata, Snapshot snapshot) {
    if (!batches.isEmpty()) {
      int minOrdinal = batches.stream().mapToInt(Batch::getOrdinal).min().getAsInt();
      ValidationException.check(
          minOrdinal == 0, "Batches must start at ordinal 0. Current min ordinal: %d", minOrdinal);

      int maxOrdinal = batches.stream().mapToInt(Batch::getOrdinal).max().getAsInt();

      ValidationException.check(
          maxOrdinal - minOrdinal == batches.size() - 1,
          "Batches must be sequential with no gaps. Current min ordinal: %d current max ordinal: %d with %d batches",
          minOrdinal,
          maxOrdinal,
          batches.size());
    }
    super.validate(currentMetadata, snapshot);
  }

  private static class Batch {
    private final List<DataFile> newDataFiles = Lists.newArrayList();
    private final List<DeleteFile> newDeleteFiles = Lists.newArrayList();
    private final int ordinal;

    /**
     * Creates a new set of updates to a specific batch
     *
     * @param ordinal the batch ordinal
     */
    Batch(int ordinal) {
      this.ordinal = ordinal;
    }

    public void add(DataFile dataFile) {
      newDataFiles.add(dataFile);
    }

    public void add(DeleteFile deleteFile) {
      newDeleteFiles.add(deleteFile);
    }

    public List<DataFile> getNewDataFiles() {
      return newDataFiles;
    }

    public List<DeleteFile> getNewDeleteFiles() {
      return newDeleteFiles;
    }

    public int getOrdinal() {
      return ordinal;
    }
  }
}
