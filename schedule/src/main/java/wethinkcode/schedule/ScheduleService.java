package wethinkcode.schedule;

import java.time.LocalTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import wethinkcode.loadshed.common.mq.MqTopicReceiver;
import wethinkcode.loadshed.common.transfer.DayDO;
import wethinkcode.loadshed.common.transfer.ScheduleDO;
import wethinkcode.loadshed.common.transfer.SlotDO;
import wethinkcode.stage.StageService;

/**
 * I provide a REST API providing the current loadshedding schedule for a given
 * town (in a specific province) at a given loadshedding stage.
 *
 * Exercise 5: I now track the current stage myself — listening on the stage Topic
 * for updates, and fetching it via HTTP on startup — so callers need not pass it.
 */
public class ScheduleService
{
    private static final Logger LOGGER = Logger.getLogger( "loadshed.schedule" );

    public static final int DEFAULT_STAGE = 0;
    public static final int DEFAULT_PORT  = 7002;
    public static final String MQ_TOPIC   = "stage";

    private static final String STAGE_SVC_URL =
        "http://localhost:" + StageService.DEFAULT_PORT;

    private int loadSheddingStage = DEFAULT_STAGE;
    private Javalin server;
    private int servicePort;
    private MqTopicReceiver mqReceiver;

    public static void main( String[] args ){
        final ScheduleService svc = new ScheduleService().initialise();
        svc.start();
    }

    @VisibleForTesting
    ScheduleService initialise(){
        // 1. Startup HTTP fetch of current stage (RPC fallback)
        fetchStageFromService();
        // 2. HTTP server
        server = initHttpServer();
        // 3. MQ topic subscription on daemon thread — never blocks startup
        subscribeToStageTopicAsync();
        return this;
    }

    @VisibleForTesting
    ScheduleService initialise( MqTopicReceiver receiver ){
        mqReceiver = receiver;
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
        if ( mqReceiver != null ) mqReceiver.close();
    }

    public void run(){
        server.start( servicePort );
    }

    // ── Stage tracking ────────────────────────────────────────────────────────

    private void fetchStageFromService(){
        try {
            final HttpResponse<JsonNode> response =
                Unirest.get( STAGE_SVC_URL + "/stage" ).asJson();
            if ( response.isSuccess() ) {
                loadSheddingStage = response.getBody().getObject().getInt( "stage" );
                LOGGER.info( "Fetched initial stage from StageService: " + loadSheddingStage );
            }
        } catch ( Exception e ) {
            LOGGER.log( Level.WARNING,
                "Could not reach StageService on startup — defaulting to stage 0", e );
        }
    }

    private void subscribeToStageTopicAsync(){
        final Thread t = new Thread( () -> {
            try {
                mqReceiver = new MqTopicReceiver();
                mqReceiver.init( MQ_TOPIC, message -> {
                    try {
                        if ( message instanceof javax.jms.TextMessage tm ) {
                            final int newStage = new ObjectMapper()
                                .readTree( tm.getText() ).get( "stage" ).asInt();
                            loadSheddingStage = newStage;
                            LOGGER.info( "Stage updated via MQ topic: " + loadSheddingStage );
                        }
                    } catch ( Exception e ) {
                        LOGGER.log( Level.WARNING, "Failed to parse stage message", e );
                    }
                } );
                LOGGER.info( "Subscribed to stage topic on MQ" );
            } catch ( Exception e ) {
                LOGGER.log( Level.WARNING,
                    "Could not connect to MQ — stage topic updates disabled", e );
            }
        }, "mq-stage-listener" );
        t.setDaemon( true );
        t.start();
    }

    // ── HTTP endpoints ────────────────────────────────────────────────────────

    private Javalin initHttpServer(){
        return Javalin.create()
            .get( "/{province}/{town}/{stage}", this::getSchedule )
            .get( "/{province}/{town}",         this::getDefaultSchedule );
    }

    private Context getSchedule( Context ctx ){
        final String province = ctx.pathParam( "province" );
        final String townName = ctx.pathParam( "town" );
        final String stageStr = ctx.pathParam( "stage" );

        if ( province.isEmpty() || townName.isEmpty() || stageStr.isEmpty() )
            return ctx.status( HttpStatus.BAD_REQUEST );

        final int stage = Integer.parseInt( stageStr );
        if ( stage < 0 || stage > 8 )
            return ctx.status( HttpStatus.BAD_REQUEST );

        final Optional<ScheduleDO> schedule = getSchedule( province, townName, stage );
        ctx.status( schedule.isPresent() ? HttpStatus.OK : HttpStatus.NOT_FOUND );
        return ctx.json( schedule.orElseGet( ScheduleService::emptySchedule ) );
    }

    /** Exercise 5: uses internally-tracked stage — no stage param needed. */
    private Context getDefaultSchedule( Context ctx ){
        final String province = ctx.pathParam( "province" );
        final String townName = ctx.pathParam( "town" );
        if ( province.isEmpty() || townName.isEmpty() )
            return ctx.status( HttpStatus.BAD_REQUEST );

        final Optional<ScheduleDO> schedule = getSchedule( province, townName, loadSheddingStage );
        ctx.status( schedule.isPresent() ? HttpStatus.OK : HttpStatus.NOT_FOUND );
        return ctx.json( schedule.orElseGet( ScheduleService::emptySchedule ) );
    }

    Optional<ScheduleDO> getSchedule( String province, String town, int stage ){
        return province.equalsIgnoreCase( "Mars" )
            ? Optional.empty()
            : Optional.of( mockSchedule() );
    }

    private static ScheduleDO mockSchedule(){
        final List<SlotDO> slots = List.of(
            new SlotDO( LocalTime.of( 2,  0 ), LocalTime.of( 4,  0 ) ),
            new SlotDO( LocalTime.of( 10, 0 ), LocalTime.of( 12, 0 ) ),
            new SlotDO( LocalTime.of( 18, 0 ), LocalTime.of( 20, 0 ) )
        );
        final List<DayDO> days = List.of(
            new DayDO( slots ), new DayDO( slots ),
            new DayDO( slots ), new DayDO( slots )
        );
        return new ScheduleDO( days );
    }

    private static ScheduleDO emptySchedule(){
        return new ScheduleDO( Collections.emptyList() );
    }
}
