package wethinkcode.stage;


import com.google.common.annotations.VisibleForTesting;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import wethinkcode.loadshed.common.mq.MqTopicSender;
import wethinkcode.loadshed.common.transfer.StageDO;
import wethinkcode.loadshed.spikes.TopicSender;

/**
 * I provide a REST API that reports the current loadshedding "stage". I provide two endpoints:
 * <dl>
 * <dt>GET /stage
 * <dd>report the current stage of loadshedding as a JSON serialisation of a {@code StageDO} data/transfer object
 * <dt>POST /stage
 * <dd>set a new loadshedding stage/level by POSTing a JSON-serialised {@code StageDO} instance as the body of the
 * request.
 * </ul>
 */
public class StageService
{
    public static final int DEFAULT_STAGE = 0; // no loadshedding. Ha!

    public static final int DEFAULT_PORT = 7001;

    public static final String MQ_TOPIC_NAME = "stage";

    public static void main( String[] args ){
        final StageService svc = new StageService().initialise();
        svc.start();
    }

    private int loadSheddingStage;

    private Javalin server;

    private int servicePort;

    private MqTopicSender sender;

    @VisibleForTesting
    StageService initialise(){
        return initialise( DEFAULT_STAGE );
    }

    @VisibleForTesting
    StageService initialise( int initialStage ){
        loadSheddingStage = initialStage;
        assert loadSheddingStage >= 0;

        server = initHttpServer();
        return this;
    }
    public StageService initialise(int initialStage, MqTopicSender sender) {
        loadSheddingStage = initialStage;
        assert loadSheddingStage >= 0;
        this.sender = sender;
        server = initHttpServer();
        return this;
    }

    public void start(){
        start( DEFAULT_PORT );
    }

    @VisibleForTesting
    void start( int networkPort ){
        servicePort = networkPort;
        run();
    }

    public void stop(){
        server.stop();
    }

    public void run(){
        server.start( servicePort );
    }

    private Javalin initHttpServer(){
        return Javalin.create()
            .get( "/stage", this::getCurrentStage )
            .post( "/stage", this::setNewStage );
    }

    private Context getCurrentStage( Context ctx ){
        return ctx.json( new StageDO( loadSheddingStage ) );
    }

    private Context setNewStage( Context ctx ){
        final StageDO stageData = ctx.bodyAsClass( StageDO.class );
        final int newStage = stageData.getStage();
        if( newStage >= 0 ){
            loadSheddingStage = newStage;
            broadcastStageChangeEvent( ctx );
            ctx.status( HttpStatus.OK );
        }else{
            ctx.status( HttpStatus.BAD_REQUEST );
        }
        return ctx.json( new StageDO( loadSheddingStage ) );
    }

    private void broadcastStageChangeEvent(Context ctx) {
        final StageDO stageData = ctx.bodyAsClass(StageDO.class);
        if (sender != null) {
            sender.send(String.valueOf(stageData.getStage()));
        } else {
            final MqTopicSender mqSender = new MqTopicSender().init(MQ_TOPIC_NAME);
            mqSender.send(String.valueOf(stageData.getStage()));
            mqSender.close();
        }
    }

}
