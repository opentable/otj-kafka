package com.opentable.kafka.logging;

import com.opentable.logging.CommonLogFields;

public final class OTKafkaHeaders {
    public static final String REFERRING_HOST = "ot-referring-host".intern();
    public static final String REFERRING_INSTANCE_NO = "ot-referring-instance_no".intern();
    public static final String REFERRING_SERVICE = "ot-referring-service".intern();
    public static final String REQUEST_ID = CommonLogFields.REQUEST_ID_KEY.intern();
    public static final String TRACE_FLAG = "ot-trace-flag".intern();
    public static final String ENV = "ot-env".intern();
    public static final String ENV_FLAVOR = "ot-env-flavor".intern();

    private OTKafkaHeaders() {
    }
}
