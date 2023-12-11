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

import java.util.List;
import java.util.Set;
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
    return super.nextSnapshotSequenceNumber(base) + batches.size() - 1;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base, Snapshot snapshot) {
    if (requiresApply && !batches.isEmpty()) {
      long startingSequenceNumber = base.nextSequenceNumber();
      int i = 0;
      while (i < batches.size()) {
        Batch batch = batches.get(i);
        long dataSequenceNumber = startingSequenceNumber + i;
        batch.getNewDataFiles().forEach(f -> add(f, dataSequenceNumber));
        batch.getNewDeleteFiles().forEach(f -> add(f, dataSequenceNumber));
        i += 1;
      }
      requiresApply = false;
    }
    return super.apply(base, snapshot);
  }

  @Override
  public StreamingUpdate newBatch() {
    if (batches.isEmpty() || !batches.get(batches.size() - 1).isEmpty()) {
      // Only add a new batch if the there isn't one or there is one, and it's not empty
      // Otherwise, we will have empty batches.
      batches.add(new Batch());
    }
    return this;
  }

  @Override
  public StreamingUpdate addFile(DataFile dataFile) {
    getBatch().add(dataFile);
    return this;
  }

  @Override
  public StreamingUpdate addFile(DeleteFile deleteFile) {
    getBatch().add(deleteFile);
    return this;
  }

  private Batch getBatch() {
    if (batches.isEmpty()) {
      newBatch();
    }
    return batches.get(batches.size() - 1);
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

  private static class Batch {
    private final List<DataFile> newDataFiles = Lists.newArrayList();
    private final List<DeleteFile> newDeleteFiles = Lists.newArrayList();

    /** Creates a new set of updates to a specific batch */
    Batch() {}

    void add(DataFile dataFile) {
      newDataFiles.add(dataFile);
    }

    void add(DeleteFile deleteFile) {
      newDeleteFiles.add(deleteFile);
    }

    List<DataFile> getNewDataFiles() {
      return newDataFiles;
    }

    List<DeleteFile> getNewDeleteFiles() {
      return newDeleteFiles;
    }

    boolean isEmpty() {
      return newDataFiles.isEmpty() && newDeleteFiles.isEmpty();
    }
  }
}
