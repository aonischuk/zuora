/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.zuora.client.ZuoraRestClient;
import io.cdap.plugin.zuora.plugin.batch.source.PageIterator;
import io.cdap.plugin.zuora.plugin.batch.source.ZuoraSourceTransformer;
import io.cdap.plugin.zuora.plugin.batch.source.ZuoraSplitArgument;
import io.cdap.plugin.zuora.plugin.common.BaseConfig;
import io.cdap.plugin.zuora.restobjects.ObjectHelper;
import io.cdap.plugin.zuora.restobjects.ObjectInfo;
import io.cdap.plugin.zuora.restobjects.objects.BaseObject;
import io.cdap.plugin.zuora.restobjects.objects.Pair;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of Spark receiver to receive Zuora push topic events
 */
public class ZuoraReceiver extends Receiver<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(ZuoraReceiver.class);
  private static final String RECEIVER_THREAD_NAME = "zuora_api_listener";
  private final ZuoraStreamingSourceConfig config;

  ZuoraReceiver(ZuoraStreamingSourceConfig config) throws IOException {
    super(StorageLevel.MEMORY_AND_DISK_2());
    this.config = config;
  }

  @Override
  public void onStart() {
    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
      .setNameFormat(RECEIVER_THREAD_NAME + "-%d")
      .build();

    Executors.newSingleThreadExecutor(namedThreadFactory).submit(this::receive);
  }

  @Override
  public void onStop() {
    // There is nothing we can do here as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  private Map<String, PageIterator> objectIterators = new HashMap<>();

  private void receive() {
    try {
      List<ZuoraSplitArgument> splitArguments = config.getObjectsWithSchema();
      Integer minutesToSleep = config.getPullFrequency().getMinutesValue();
      ZuoraRestClient client = new ZuoraRestClient(config);

      while (!isStopped()) {
        for (ZuoraSplitArgument splitArgument : splitArguments) {
          String objectName = splitArgument.getObjectName();
          String objectSchemaString = splitArgument.getObjectSchema();

          Schema objectSchema;
          if (!config.isSingleObjectMode()) {
            Schema schema = Schema.parseJson(objectSchemaString);
            List<Pair<String, Schema.Type>> multiObjectFields = new ArrayList<>();
            multiObjectFields.add(new Pair<>(BaseConfig.OBJECT_NAME_FIELD, Schema.Type.STRING));

            objectSchema = ObjectHelper.alterSchema(objectName, schema, multiObjectFields);
          } else {
            objectSchema = Schema.parseJson(objectSchemaString);
          }

          if (!objectIterators.containsKey(objectName)) {
            ObjectInfo objectInfo = ObjectHelper.getObjectInfo(objectName);
            objectIterators.put(objectName, new PageIterator(client, objectInfo, config.getArguments()));
          } else {
            objectIterators.get(objectName).reloadCurrentPage();
          }

          PageIterator pageIterator = objectIterators.get(objectName);

          while (pageIterator.hasNext()) {
            BaseObject object = pageIterator.next();

            if (!config.isSingleObjectMode()) {
              object.addCustomField(BaseConfig.OBJECT_NAME_FIELD, objectName);
            }

            store(ZuoraSourceTransformer.transform(object, objectSchema));
          }
        }
        LOG.debug(String.format("Waiting for '%d' minutes to pull.", minutesToSleep));
        Thread.sleep(TimeUnit.MINUTES.toMillis(minutesToSleep));
      }
    } catch (Exception e) {
      String errorMessage = "Exception while receiving messages from Zuora";
      // Since it's top level method of thread, we need to log the exception or it will be unseen
      LOG.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }
  }
}
