package wethinkcode.web;

import com.google.common.annotations.VisibleForTesting;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.staticfiles.Location;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import wethinkcode.places.PlaceNameService;
import wethinkcode.schedule.ScheduleService;
import wethinkcode.stage.StageService;

import kong.unirest.HttpResponse;

/**
 * I am the front-end web server for the LightSched project.
 * <p>
 * Remember that we're not terribly interested in the web front-end part of this server, more in the way it communicates
 * and interacts with the back-end services.
 */
public class WebService
{

    public static final int DEFAULT_PORT = 80;

    public static final String STAGE_SVC_URL = "http://localhost:" + StageService.DEFAULT_PORT;

    public static final String PLACES_SVC_URL = "http://localhost:" + PlaceNameService.DEFAULT_PORT;

    public static final String SCHEDULE_SVC_URL = "http://localhost:" + ScheduleService.DEFAULT_PORT;

    private static final String PAGES_DIR = "/html";

    private int LOAD_SHEDDING_STAGE;

    public static void main( String[] args ){
        final WebService svc = new WebService().initialise();
        svc.start();
    }

    private Javalin server;

    private int servicePort;

    @VisibleForTesting
    WebService initialise(){
        // FIXME: Initialise HTTP client, MQ machinery and server from here
        server = configureHttpServer();
        configureHttpClient();
        server.get("/",this::mainPage);
        server.post("/towns",this::townsRequest);
        server.post("/schedule",this::scheduleRequest);
        server.post("/updateStage",this::updateStage);
        return this;

    }

    private void updateStage(Context context) {
    }

    private void scheduleRequest(Context context) {
    }

    private void townsRequest(Context context) {
    }

    private void mainPage(Context context) {
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

    private void configureHttpClient(){
        HttpResponse<JsonNode> responseStage = stageHttpRequest();
        int httpStage = responseStage.getBody().getObject().getInt( "stage" );
        LOAD_SHEDDING_STAGE = httpStage;
    }

    private HttpResponse<JsonNode> stageHttpRequest() {
        return Unirest.get(STAGE_SVC_URL + "/stage")
                .asJson();
    }

    private Javalin configureHttpServer(){
        return Javalin.create(config -> {
            config.addStaticFiles(PAGES_DIR, Location.CLASSPATH);
        });
    }

}
