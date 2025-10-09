FROM gradle:9.1-jdk17 AS builder

WORKDIR /src

COPY . .

RUN gradle :sample:clean :sample:installDist -x test --no-daemon

FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY --from=builder /src/sample/build/install/sample /app

ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=80.0"

ENTRYPOINT ["/app/bin/sample"]

CMD []
