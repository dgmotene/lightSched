package wethinkcode.loadshed.alert;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

import kong.unirest.Unirest;
import wethinkcode.loadshed.common.mq.MQ;
import wethinkcode.loadshed.common.mq.MqQueueReceiver;

public class AlertService {

    private static final Logger LOGGER = Logger.getLogger("loadshed.alert"); // Good subsystem logger

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        MqQueueReceiver receiver = new MqQueueReceiver(MQ.ALERT_QUEUE);

        try {
            final MessageConsumer listener = receiver.init(MQ.URL);
            listener.setMessageListener(message -> {
                if (message instanceof TextMessage tm) {
                    try {
                        String alertText = "🚨 *LightSched Alert:* " + tm.getText();

                        // Console
                        System.out.println(alertText);
                        LOGGER.info(alertText);

                        // ntfy.sh (Exercise 4)
                        sendToNtfy(alertText);

                    } catch (JMSException e) {
                        LOGGER.log(Level.SEVERE, "Failed to read alert message", e);
                    }
                }
            });

            LOGGER.info("AlertService started - listening on queue: " + MQ.ALERT_QUEUE);

        } catch (JMSException e) {
            LOGGER.log(Level.SEVERE, "Failed to start AlertService", e);
            throw new RuntimeException(e);
        }
    }

    private static void sendToNtfy(String message) {
        try {

            String topic = "dalton123topic";
            Unirest.post("https://ntfy.sh/" + topic)
                    .header("Content-Type", "text/plain")
                    .header("Title", "LightSched Alert")
                    .header("Tags", "warning,alert")
                    .header("Priority", "high")
                    .body(message)
                    .asString();

            System.out.println("→ Sent to ntfy");

        } catch (Exception e) {
            System.err.println("Failed to send to ntfy: " + e.getMessage());
        }
    }
}