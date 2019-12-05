/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.zuora.plugin.streaming.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import org.apache.spark.streaming.api.java.JavaDStream;
import java.io.IOException;

/**
 * Streams data updates from Zuora.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(ZuoraStreamingSource.NAME)
@Description(ZuoraStreamingSource.DESCRIPTION)
public class ZuoraStreamingSource extends StreamingSource<StructuredRecord> {
  static final String NAME = "Zuora";
  static final String DESCRIPTION = "Streams data updates from Zuora";
  private ZuoraStreamingSourceConfig config;

  public ZuoraStreamingSource(ZuoraStreamingSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector); // validate when macros are not substituted
    collector.getOrThrowException();

    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema(false));
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws IOException {
    FailureCollector collector = streamingContext.getFailureCollector();
    config.validate(collector); // validate when macros are substituted
    collector.getOrThrowException();

    return ZuoraStreamingUtil.getStream(config, streamingContext);
  }
}
