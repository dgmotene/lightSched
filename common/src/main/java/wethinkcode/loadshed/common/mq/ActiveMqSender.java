package wethinkcode.loadshed.common.mq;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * I am a general-purpose ActiveMQ sender that works with both Queues and Topics.
 */
public class ActiveMqSender implements AutoCloseable {

    private final MQ.DestinationType type;
    private Connection connection;
    private Session session;

    public ActiveMqSender( MQ.DestinationType type ) {
        this.type = type;
    }

    public void send( String destinationName, String text ) throws JMSException {
        connect();
        final Destination dest = ( type == MQ.DestinationType.TOPIC )
            ? session.createTopic( destinationName )
            : session.createQueue( destinationName );
        final MessageProducer producer = session.createProducer( dest );
        producer.send( session.createTextMessage( text ) );
        producer.close();
    }

    private void connect() throws JMSException {
        if ( connection == null ) {
            final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory( MQ.URL );
            connection = factory.createConnection( MQ.USER, MQ.PASSWD );
            connection.start();
            session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );
        }
    }

    @Override
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
