package com.opentable.kafka.logging;

import com.opentable.logging.CommonLogFields;

public final class OTKafkaHeaders {
    //DMITRY Any reason
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
