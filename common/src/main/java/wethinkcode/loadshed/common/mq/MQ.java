package wethinkcode.loadshed.common.mq;

public interface MQ
{
    String URL = "tcp://localhost:61616";
    String USER = "admin";
    String PASSWD = "admin";
    String STAGE_TOPIC = "stage";
    String ALERT_QUEUE = "alert";

    enum DestinationType { QUEUE, TOPIC }
}
