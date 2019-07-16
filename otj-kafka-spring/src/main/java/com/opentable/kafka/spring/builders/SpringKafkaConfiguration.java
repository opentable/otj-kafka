package com.opentable.kafka.spring.builders;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.opentable.kafka.builders.AppInfoEnvironmentProvider;
import com.opentable.kafka.builders.KafkaBuilderFactoryBean;
import com.opentable.service.AppInfo;
import com.opentable.service.EnvInfo;

@Configuration
@Import({
    KafkaBuilderFactoryBean.class,
    AppInfo.class,
    EnvInfo.class,
    AppInfoEnvironmentProvider.class,
    KafkaFactoryBuilderFactoryBean.class
})
public class SpringKafkaConfiguration {

}
