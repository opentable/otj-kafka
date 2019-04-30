package com.opentable.kafka.builders;

abstract class SelfTyped<SELF extends SelfTyped<SELF>> {

    /**
     * @return This instance.
     */
    abstract SELF self();

}
