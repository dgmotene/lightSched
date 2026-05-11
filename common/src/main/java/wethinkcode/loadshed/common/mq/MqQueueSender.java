package wethinkcode.loadshed.common.mq;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * I send messages to an ActiveMQ Queue (point-to-point).
 * Used by services to send alert notifications to the AlertService.
 */
public class MqQueueSender {

    private Connection connection;
    private Session session;

    public void send( String queueName, String text ) throws JMSException {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory( MQ.URL );
        connection = factory.createConnection( MQ.USER, MQ.PASSWD );
        connection.start();
        session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );

        final Destination dest = session.createQueue( queueName );
        final MessageProducer producer = session.createProducer( dest );
        producer.send( session.createTextMessage( text ) );
        producer.close();
    }

    public void close() {
        try {
            if ( session != null ) session.close();
            if ( connection != null ) connection.close();
        } catch ( JMSException e ) {
            // meh
        } finally {
            session = null;
            connection = null;
        }
    }
}
