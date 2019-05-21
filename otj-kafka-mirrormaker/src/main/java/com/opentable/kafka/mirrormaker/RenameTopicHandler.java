/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.opentable.kafka.mirrormaker;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.consumer.BaseConsumerRecord;
import kafka.tools.MirrorMaker;
// https://github.com/slavirok/mirrormaker-rename-topic-handler, https://github.com/opencore/mirrormaker_topic_rename mildly changed
// to run
//kafka-mirror-maker --consumer.config consumer.cfg --producer.config producer.cfg --whitelist "source1|source2" \
// --message.handler com.opentable.kafka.mirrormaker.RenameTopicHandler --message.handler.args "source_topic_A,target_topicA;source_topic_A,target_topic_A"
public class RenameTopicHandler implements MirrorMaker.MirrorMakerMessageHandler {
  // We have to use a simple map to avoid dependencies, so we do it old school below.
  private final Map<String, String> topicMap;

  // The mirror maker calls this with a single String argument
  // It's true we COULD use regex, but given EDA destinations follow their own "thang" not sure there is a gain
  public RenameTopicHandler(final String topics) {
    Map<String, String> map = new HashMap<>();
    if(topics != null && !topics.isEmpty()){
      for (String topicAssignment : topics.split(";")) {
        String[] topicsArray = topicAssignment.split(",");
        if (topicsArray.length == 2) {
          map.put(topicsArray[0].trim(), topicsArray[1].trim());
        }
      }
    }
    topicMap = Collections.unmodifiableMap(map);
  }

  @SuppressWarnings("deprecation")
  public List<ProducerRecord<byte[], byte[]>> handle(final BaseConsumerRecord record) {
    final Long timestamp = record.timestamp() == ConsumerRecord.NO_TIMESTAMP ? null : record.timestamp();
    return Collections.singletonList(new ProducerRecord<>(getTopicName(record.topic()),
              null, timestamp, record.key(), record.value(), record.headers()));
  }

  private String getTopicName(String currentTopicName){
    return topicMap.getOrDefault(currentTopicName, currentTopicName);
  }
}
