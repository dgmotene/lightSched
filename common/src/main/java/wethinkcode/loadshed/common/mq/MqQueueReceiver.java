package wethinkcode.loadshed.common.mq;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * I receive messages from an ActiveMQ Queue (point-to-point).
 * Used by AlertService to consume alert messages reliably.
 */
public class MqQueueReceiver {

    private final String queueName;
    private Connection connection;

    public MqQueueReceiver( String queueName ) {
        this.queueName = queueName;
    }

    public MessageConsumer init( String brokerUrl ) throws JMSException {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory( brokerUrl );
        connection = factory.createConnection( MQ.USER, MQ.PASSWD );

        final Session session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );
        final Destination dest = session.createQueue( queueName );

        final MessageConsumer consumer = session.createConsumer( dest );
        connection.start();
        return consumer;
    }

    public void close() {
        if ( connection != null ) {
            try {
                connection.close();
            } catch ( JMSException e ) {
                // meh
            } finally {
                connection = null;
            }
        }
    }
}
