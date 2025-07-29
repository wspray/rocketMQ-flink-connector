package org.apache.rocketmq.flink.sink.stream;

import java.io.Serializable;

public interface TopicSelector<T> extends Serializable {

    String getTopic(T tuple);

    String getTag(T tuple);
}
