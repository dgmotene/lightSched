package wethinkcode.loadshed.common.mq;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * I am a general-purpose ActiveMQ receiver that works with both Queues and Topics.
 * The DestinationType passed to the constructor decides which mode is used.
 */
public class ActiveMqReceiver implements AutoCloseable {

    private final MQ.DestinationType type;
    private Connection connection;
    private Session session;

    public ActiveMqReceiver( MQ.DestinationType type ) {
        this.type = type;
    }

    /** Async: set a MessageListener and return immediately. */
    public void listenOn( String destinationName, MessageListener listener ) throws JMSException {
        connect();
        final Destination dest = createDestination( destinationName );
        final MessageConsumer consumer = session.createConsumer( dest );
        consumer.setMessageListener( listener );
        connection.start();
    }

    /** Synchronous / blocking receive — waits up to 5 seconds for a message. */
    public Message receive( String destinationName ) throws JMSException {
        connect();
        final Destination dest = createDestination( destinationName );
        final MessageConsumer consumer = session.createConsumer( dest );
        connection.start();
        return consumer.receive( 5000 );
    }

    private void connect() throws JMSException {
        if ( connection == null ) {
            final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory( MQ.URL );
            connection = factory.createConnection( MQ.USER, MQ.PASSWD );
            session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );
        }
    }

    private Destination createDestination( String name ) throws JMSException {
        return ( type == MQ.DestinationType.TOPIC )
            ? session.createTopic( name )
            : session.createQueue( name );
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
