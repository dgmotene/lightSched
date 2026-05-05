package wethinkcode.loadshed.common.mq.test;

import wethinkcode.loadshed.common.mq.MqTopicSender;

public class NullTopicSender extends MqTopicSender {

    @Override
    public MqTopicSender init(String topicName) {
        return this; // don't actually connect to MQ
    }

    @Override
    public void send(String message) {
    // no-op: discard the message in tests
    }

    @Override
    public void close() {
    // no-op: nothing to close
    }
}
