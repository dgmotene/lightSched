package wethinkcode.loadshed.common.mq;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

public class MqTopicReceiver {

    private Connection connection;

    public MqTopicReceiver init(String topicName, MessageListener listener) {
        try {
            final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(MQ.URL);
            connection = factory.createConnection(MQ.USER, MQ.PASSWD);

            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Destination dest = session.createTopic(topicName);

            final MessageConsumer consumer = session.createConsumer(dest);
            consumer.setMessageListener(listener);

            connection.start();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                // meh
            } finally {
                connection = null;
            }
        }
    }
}