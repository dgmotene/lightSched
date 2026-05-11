package wethinkcode.web;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import wethinkcode.loadshed.common.mq.MQ;
import wethinkcode.loadshed.common.mq.MqTopicReceiver;
import wethinkcode.loadshed.common.mq.MqQueueSender;
import wethinkcode.loadshed.common.transfer.StageDO;
import wethinkcode.places.PlaceNameService;
import wethinkcode.schedule.ScheduleService;
import wethinkcode.stage.StageService;

/**
 * I am the front-end web server for the LightSched project.
 *
 * Exercise 4: I subscribe to the stage Topic so I track stage changes
 * without polling StageService on every request.
 *
 * Exercise 5: I call /{province}/{town} on ScheduleService — no stage
 * in the URL because ScheduleService now tracks it internally too.
 */
public class WebService
{
    private static final Logger LOGGER = Logger.getLogger( "loadshed.web" );

    public static final int DEFAULT_PORT = 7003;

    public static final String STAGE_SVC_URL    = "http://localhost:" + StageService.DEFAULT_PORT;
    public static final String PLACES_SVC_URL   = "http://localhost:" + PlaceNameService.DEFAULT_PORT;
    public static final String SCHEDULE_SVC_URL = "http://localhost:" + ScheduleService.DEFAULT_PORT;

    private int loadSheddingStage = 0;
    private Javalin server;
    private int servicePort;
    private MqTopicReceiver mqReceiver;

    public static void main( String[] args ){
        final WebService svc = new WebService().initialise();
        svc.start();
    }

    @VisibleForTesting
    WebService initialise(){
        // 1. Fetch current stage via HTTP (RPC startup call — runs before any topic message arrives)
        fetchStageFromService();
        // 2. Build and configure HTTP server
        server = configureHttpServer();
        // 3. Subscribe to stage topic for future updates (on a background thread so startup never blocks)
        subscribeToStageTopicAsync();
        return this;
    }

    @VisibleForTesting
    WebService initialise( int initialStage ){
        loadSheddingStage = initialStage;
        server = configureHttpServer();
        return this;
    }

    public void start(){
        start( DEFAULT_PORT );
    }

    @VisibleForTesting
    void start( int networkPort ){
        servicePort = networkPort;
        server.start( servicePort );
    }

    public void stop(){
        server.stop();
        if ( mqReceiver != null ) mqReceiver.close();
    }

    // ── Stage tracking ────────────────────────────────────────────────────────

    private void fetchStageFromService(){
        try {
            final HttpResponse<JsonNode> response =
                Unirest.get( STAGE_SVC_URL + "/stage" ).asJson();
            if ( response.isSuccess() ) {
                loadSheddingStage = response.getBody().getObject().getInt( "stage" );
                LOGGER.info( "Initial stage fetched from StageService: " + loadSheddingStage );
            }
        } catch ( Exception e ) {
            LOGGER.log( Level.WARNING, "StageService unreachable on startup — defaulting to stage 0", e );
        }
    }

    /** Subscribe on a daemon thread so it never blocks server startup. */
    private void subscribeToStageTopicAsync(){
        final Thread t = new Thread( () -> {
            try {
                mqReceiver = new MqTopicReceiver();
                mqReceiver.init( MQ.STAGE_TOPIC, message -> {
                    try {
                        if ( message instanceof javax.jms.TextMessage tm ) {
                            final int newStage = new ObjectMapper()
                                .readTree( tm.getText() ).get( "stage" ).asInt();
                            loadSheddingStage = newStage;
                            LOGGER.info( "Stage updated via MQ topic: " + loadSheddingStage );
                        }
                    } catch ( Exception e ) {
                        LOGGER.log( Level.WARNING, "Could not parse stage topic message", e );
                    }
                } );
                LOGGER.info( "Subscribed to stage topic on MQ" );
            } catch ( Exception e ) {
                LOGGER.log( Level.WARNING, "Could not connect to MQ — stage topic updates disabled", e );
            }
        }, "mq-stage-listener" );
        t.setDaemon( true );
        t.start();
    }

    // ── HTTP server ───────────────────────────────────────────────────────────

    private Javalin configureHttpServer(){
        return Javalin.create()
            .get( "/",                          this::mainPage )
            .get( "/stage",                     this::getStage )
            .get( "/provinces",                 this::getProvinces )
            .get( "/towns/{province}",          this::getTowns )
            .get( "/schedule/{province}/{town}", this::getSchedule );
    }

    private void mainPage( Context ctx ){
        ctx.html(
            "<h1>LightSched</h1>" +
            "<p>Current loadshedding stage: <strong>" + loadSheddingStage + "</strong></p>" +
            "<ul>" +
            "<li>GET /stage</li>" +
            "<li>GET /provinces</li>" +
            "<li>GET /towns/{province}</li>" +
            "<li>GET /schedule/{province}/{town}</li>" +
            "</ul>"
        );
    }

    private void getStage( Context ctx ){
        ctx.json( new StageDO( loadSheddingStage ) );
    }

    private void getProvinces( Context ctx ){
        try {
            final HttpResponse<JsonNode> resp =
                Unirest.get( PLACES_SVC_URL + "/provinces" ).asJson();
            ctx.status( resp.getStatus() )
               .result( resp.getBody().toString() )
               .contentType( "application/json" );
        } catch ( Exception e ) {
            LOGGER.log( Level.SEVERE, "Cannot reach PlaceNameService", e );
            sendAlert( "WebService: Cannot reach PlaceNameService at " + PLACES_SVC_URL );
            ctx.status( HttpStatus.SERVICE_UNAVAILABLE );
        }
    }

    private void getTowns( Context ctx ){
        final String province = ctx.pathParam( "province" );
        try {
            final HttpResponse<JsonNode> resp =
                Unirest.get( PLACES_SVC_URL + "/towns/" + province ).asJson();
            ctx.status( resp.getStatus() )
               .result( resp.getBody().toString() )
               .contentType( "application/json" );
        } catch ( Exception e ) {
            LOGGER.log( Level.SEVERE, "Cannot reach PlaceNameService", e );
            sendAlert( "WebService: Cannot reach PlaceNameService at " + PLACES_SVC_URL );
            ctx.status( HttpStatus.SERVICE_UNAVAILABLE );
        }
    }

    /** Exercise 5: no stage in URL — ScheduleService tracks it internally. */
    private void getSchedule( Context ctx ){
        final String province = ctx.pathParam( "province" );
        final String town     = ctx.pathParam( "town" );
        try {
            final HttpResponse<JsonNode> resp =
                Unirest.get( SCHEDULE_SVC_URL + "/" + province + "/" + town ).asJson();
            ctx.status( resp.getStatus() )
               .result( resp.getBody().toString() )
               .contentType( "application/json" );
        } catch ( Exception e ) {
            LOGGER.log( Level.SEVERE, "Cannot reach ScheduleService", e );
            sendAlert( "WebService: Cannot reach ScheduleService at " + SCHEDULE_SVC_URL );
            ctx.status( HttpStatus.SERVICE_UNAVAILABLE );
        }
    }

    // ── Alert ─────────────────────────────────────────────────────────────────

    private void sendAlert( String msg ){
        try {
            final MqQueueSender sender = new MqQueueSender();
            sender.send( MQ.ALERT_QUEUE, msg );
            sender.close();
            LOGGER.warning( "Alert sent to queue: " + msg );
        } catch ( Exception e ) {
            LOGGER.log( Level.SEVERE, "Failed to send alert to MQ queue", e );
        }
    }
}
