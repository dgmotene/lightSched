package wethinkcode.stage;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import kong.unirest.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.*;
import wethinkcode.loadshed.common.mq.MqTopicSender;
import wethinkcode.loadshed.common.transfer.StageDO;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("expensive")
public class StageServiceMQTest
{
    public static final int TEST_PORT = getRandomNumber();

    public static int getRandomNumber() {
        Random rand = new Random();
        return rand.nextInt(7000, 8000);
    }

    private static StageService server;
    private static ActiveMQConnectionFactory factory;
    private static BrokerService broker;

    private Connection mqConnection;
    private CountDownLatch latch;
    private StageDO receivedStage;

    @BeforeAll
    public static void startInfrastructure() throws Exception {
        startMsgQueue();
        startStageSvc();
    }

    @AfterAll
    public static void cleanup() throws Exception {
        server.stop();
        broker.stop();
    }

    @BeforeEach
    public void connectMqListener() throws JMSException {
        latch = new CountDownLatch(1);
        receivedStage = null;

        mqConnection = factory.createConnection();
        final Session session = mqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Destination dest = session.createTopic(StageService.MQ_TOPIC_NAME);

        final ObjectMapper mapper = new ObjectMapper();
        final MessageConsumer receiver = session.createConsumer(dest);
        receiver.setMessageListener(message -> {
            if (message instanceof TextMessage) {
                try {
                    receivedStage = mapper.readValue(((TextMessage) message).getText(), StageDO.class);
                    latch.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        mqConnection.start();
    }

    @AfterEach
    public void closeMqConnection() throws JMSException {
        if (mqConnection != null) {
            mqConnection.close();
            mqConnection = null;
        }
    }

    @Test
    public void sendMqEventWhenStageChanges() throws Exception {
        final HttpResponse<StageDO> startStage = Unirest.get(serverUrl() + "/stage")
                .asObject(StageDO.class);
        assertEquals(HttpStatus.OK, startStage.getStatus());

        final int newStage = startStage.getBody().getStage() + 1;

        final HttpResponse<JsonNode> changeStage = Unirest.post(serverUrl() + "/stage")
                .header("Content-Type", "application/json")
                .body(new StageDO(newStage))
                .asJson();
        assertEquals(HttpStatus.OK, changeStage.getStatus());

        boolean messageArrived = latch.await(3, TimeUnit.SECONDS);

        assertNotNull(receivedStage, "Expected an MQ message to be received");
        assertEquals(newStage, receivedStage.getStage());
    }

    private static void startMsgQueue() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
    }

    private static void startStageSvc() {
        final String VM_URL = "vm://localhost?create=false";
        MqTopicSender testSender = new MqTopicSender().init(VM_URL, StageService.MQ_TOPIC_NAME);
        server = new StageService().initialise(0, testSender);
        server.start(TEST_PORT);
    }

    private String serverUrl() {
        return "http://localhost:" + TEST_PORT;
    }
}