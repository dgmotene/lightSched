# LightSched

A distributed load-shedding schedule system built with Java, Javalin, and ActiveMQ. Four backend services communicate over HTTP and a message queue, with a simple web front-end.

## Services

| Service | Port | Description |
|---|---|---|
| PlaceNameService | 7000 | Serves SA provinces and towns from a CSV file |
| StageService | 7001 | Tracks the current load-shedding stage (0–8) |
| ScheduleService | 7002 | Returns a 4-day load-shedding schedule per town |
| WebService | 7003 | HTML front-end, orchestrates the other services |
| AlertService | — | Listens on the MQ alert queue and logs fault notices |

## Requirements

- Java 17+
- Maven 3.8+
- ActiveMQ 6.x Classic running on `localhost:61616`

## Build

```bash
mvn clean install
```

## Run

Start ActiveMQ first, then each service in a separate terminal:

```bash
java -jar places/target/places.jar -f places.csv
java -jar stage/target/stage.jar
java -jar schedule/target/schedule.jar
java -jar alert/target/alert.jar
java -jar web/target/web.jar
```

Open **http://localhost:7003** in your browser.

## How it works

- Stage changes are published to an ActiveMQ **topic** (`stage`). Both the ScheduleService and WebService subscribe to it so they always know the current stage without polling.
- Fault notifications go to an ActiveMQ **queue** (`alert`) for guaranteed delivery to the AlertService.
- If a backend service is unavailable on startup, the other services log a warning and continue rather than crashing.
