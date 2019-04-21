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
package com.opentable.kafka.builders;

public class TODO {
    /**
     * 1. Review logic with Dmitry.
     *
     * I've changed a lot of it, mostly for the better (I Hope). Generics flow cleanly (although a bit of a hack was needed - see the factory beans
     * for explanation), and the composition and building and validation is cleaner - at the cost of more boilerplate. Considerable rewrites of
     * all builders and logging. Also much general scaffolding work was cleaned up.
     *
     * I believe my reasonning in all cases is sound, but I ask you to take the time to look over it, and then let's discuss.
     *
     * 2. Discuss issues with Spark etc - does EnvironmentProvider (and its mockable SettableEnvironmentProvider) do the trick
     *
     * I recognize the issues if you have NO way to build the bean manually in such an environment, but I assume you do. So I rely on Spring
     * for the happy path, and a non spring friendly provider unfortunately has to laboriously inject the manually built SettableEnvironmentProvider.
     *
     * I wonder what we'll do for EDA. Ideally we make it Spring-required and break some compatibility but carefully. The new idiom works so
     * well with simple @InjectKafkaBuilderBeans, and then the only requirement for Bus I believe lies below under the "unique name". But I'll
     * need to look at that code again to see.
     *
     * 3. improve tests
     *
     * They need more robust testing and look a bit incomplete
     *
     * 4. Discuss MDC put behavior. I've removed all of this.
     *
     * I believe the reasoning wasn't bad - why not restore this. But the unconditional nature makes me uneasy. It's possible you'd be
     * replacing stuff that is already there. Also, I didn't understand the try/finally at all (put, remove) in both producer and consumer
     * - since the builder already
     * includes all the relevant information. It's guaranteed to tromp on preexisting values, I think you are trying to just wrap any errors?
     * It just feels wrong, and maybe we can revisit this later (and with a toggle), because
     * I do see the theoretical value.
     *
     * 5. Consider metric namespace rooting and cardinality. This is, sigh the hardest, because here kafka's tagged oriented architecture
     * clashes with Graphite a fair bit
     *
     * My belief re #5 is the following.
     *
     * Dmitry chooses
     *          clientId.[groupId].sortedTags.metricGroup.metricName
     *
     *  There are several problems with this. But consider the following: We have a unique namespace per MACHINE instance, our goal is
     *  merely to subspace per CONSUMER or PRODUCER instance. That's it. Easy eh? Well ...
     *
     *  - ClientId appears to be logical, but looking at much code used, there's often a UUID introduced for clientId or TaskHost.
     *  So, no good - we must not use this
     *  - groupId is mostly sane, but shared among ALL instances on the same machine. So since these are gauges, it would be totally confusing.
     *  It's also optional, so not 100% perfect anyway.
     *  We thus discard groupId
     *
     *  We pause for a reflection on the sorted tags issue. A quick dump shows client-id is the most common one, and Dmitry already discards this
     *  The other common one is topic. That's PRETTY useful to namespace by albeit not unique, and not provided on al metrics.l
     *  Still it's no good as a terminal matcher, and is only a grouping matcher, more or less but slightly better than groupId. And not all
     *  metrics are topic-tagged. So a parent is still needed. Also I am not sure what other tags are used. Looking at the code, both
     *  topic and partition can exist, right now the code would generate partition.topic, which is arguable at best. So more work needs to
     *  be considered here.
     *
     *  We conclude, regretfully, that the factorybean and builders must require a unique name per instance. One can use a default, but
     *  only at the risk of collisions in metrics in a multi-consumer/producer environment. Also check if producer and consumer
     *  metrics can be deconvolved at all? Thhat would make it even more urgent to namespace - you are solely relying on clientId, which
     *  as discussed is both too unique and not unique enough ;)
     *
     *  Anyway initial look seems to say: Don't use clientId/groupId, possibly use topic. And require a master name per instance.
     *  However, the good news (fwiw) is Dmitry already provided for this with his namespaced properties.
     *  (I would recommend removing the DEFAULT constructor though - what if someone only needs a single Consumer or Producer now, but adds
     *  one later - they would be locked in. [or have to change alerts])
     *
     *  https://github.com/apakulov/kafka-graphite/blob/master/kafka-graphite-clients/src/main/java/org/apache/kafka/common/metrics/GraphiteReporter.java
     *
     *  https://github.com/RTBHOUSE/kafka-graphite-reporter/blob/master/src/main/java/com/rtbhouse/reporter/KafkaClientMetricsReporter.java
     *
     * 6. I've noted in the code several hot spots in metrics that I worry about performance wise. Also I synchronized all methods, because
     * I see no guarantee of thread safety
     */
}
