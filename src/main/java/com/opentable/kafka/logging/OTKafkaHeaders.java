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
    public static final String REFERRING_HOST = "ot-referring-host";
    public static final String REFERRING_INSTANCE_NO = "ot-referring-instance_no";
    public static final String REFERRING_SERVICE = "ot-referring-service";
    public static final String REQUEST_ID = CommonLogFields.REQUEST_ID_KEY;
    public static final String TRACE_FLAG = "ot-trace-flag";
    public static final String ENV = "ot-env";
    public static final String ENV_FLAVOR = "ot-env-flavor";

    private OTKafkaHeaders() {
    }
}
