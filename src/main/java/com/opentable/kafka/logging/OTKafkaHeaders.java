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
package com.opentable.kafka.logging;

import com.opentable.logging.CommonLogFields;

public final class OTKafkaHeaders {
    // The namespacing logic is as follows
    // 1. All library usage (this library or others) preface with ot-
    // 2. Then the library name (kafkalibrary in this case)
    // 3. Then whatever you want
    // 4. This helps prevent collision
    public static final String REFERRING_HOST = "ot-kafkalibrary-referring-host";
    public static final String REFERRING_INSTANCE_NO = "ot-kafkalibrary-referring-instance_no";
    public static final String REFERRING_SERVICE = "ot-kafkalibrary-referring-service";
    public static final String REQUEST_ID = "otkafkalibrary-request-id";
    public static final String TRACE_FLAG = "ot-kafkalibrary-trace-flag";
    public static final String ENV = "ot-kafkalibrary-env";
    public static final String ENV_FLAVOR = "ot-kafkalibrary-env-flavor";

    private OTKafkaHeaders() {
    }
}
