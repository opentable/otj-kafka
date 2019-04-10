package com.opentable.kafka.logging;

import java.util.UUID;

import com.opentable.logging.CommonLogFields;
import com.opentable.logging.CommonLogHolder;

class KafkaCommonLogHolder {

    private static CommonLogFields INSTANCE = new CommonLogFieldsInternal();

    private KafkaCommonLogHolder() {
    }

    public static String getHost() {
        return INSTANCE.getHost();
    }

    public static  Integer getInstanceNo() {
        return INSTANCE.getInstanceNo();
    }

    public static  String getOtEnv() {
        return INSTANCE.getOtEnv();
    }

    public static  String getOtEnvType() {
        return INSTANCE.getOtEnvType();
    }

    public static  String getOtEnvLocation() {
        return INSTANCE.getOtEnvLocation();
    }

    public static  String getOtEnvFlavor() {
        return INSTANCE.getOtEnvFlavor();
    }

    public static String getServiceType() {
        return CommonLogHolder.getServiceType();
    }


        private static class CommonLogFieldsInternal  implements CommonLogFields {

        @Override
        public String getTimestamp() {
            return null;
        }

        @Override
        public UUID getMessageId() {
            return null;
        }

        @Override
        public String getServiceType() {
            return null;
        }

        @Override
        public String getLogTypeName() {
            return null;
        }

        @Override
        public String getLogClass() {
            return null;
        }

        @Override
        public String getSeverity() {
            return null;
        }

        @Override
        public String getMessage() {
            return null;
        }

        @Override
        public String getThreadName() {
            return null;
        }

        @Override
        public String getThrowable() {
            return null;
        }

        @Override
        public String getLoglov3Otl() {
            return null;
        }
    }
}
