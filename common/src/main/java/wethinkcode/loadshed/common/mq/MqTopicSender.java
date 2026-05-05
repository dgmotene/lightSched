package wethinkcode.loadshed.common.mq;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

public class MqTopicSender {

    private Connection connection;
    private Session session;
    private MessageProducer producer;

    public MqTopicSender init(String topicName) {
        return init(MQ.URL, topicName);
    }

    public MqTopicSender init(String brokerUrl, String topicName) {
        try {
            final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
            connection = factory.createConnection(MQ.USER, MQ.PASSWD);
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Destination dest = session.createTopic(topicName);
            producer = session.createProducer(dest);
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public void send(String message) {
        try {
            final TextMessage msg = session.createTextMessage(message);
            producer.send(msg);
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            if (producer != null) producer.close();
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (JMSException e) {
            // meh
        } finally {
            producer = null;
            session = null;
            connection = null;
        }
    }
}