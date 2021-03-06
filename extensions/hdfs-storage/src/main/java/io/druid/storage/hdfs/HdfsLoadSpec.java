/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
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

package io.druid.storage.hdfs;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.metamx.common.ISE;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.LoadSpec;
import io.druid.segment.loading.SegmentLoadingException;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.Map;

/**
 *
 */
@JsonTypeName(HdfsStorageDruidModule.SCHEME)
public class HdfsLoadSpec implements LoadSpec
{
  private final Path path;
  final HdfsDataSegmentPuller puller;
  @JsonCreator
  public HdfsLoadSpec(
      @JacksonInject HdfsDataSegmentPuller puller,
      @JsonProperty(value = "path", required = true) String path
  ){
    Preconditions.checkNotNull(path);
    this.path = new Path(path);
    this.puller = puller;
  }
  @JsonProperty("path")
  public final String getPathString(){
    return path.toString();
  }

  @Override
  public LoadSpecResult loadSegment(File outDir) throws SegmentLoadingException
  {
    return new LoadSpecResult(puller.getSegmentFiles(path, outDir).size());
  }
}
