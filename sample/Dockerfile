FROM gradle:9.1-jdk17 AS builder

WORKDIR /src

COPY . .

RUN gradle :sample:clean :sample:installDist -x test --no-daemon

FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY --from=builder /src/sample/build/install/sample /app

ENTRYPOINT ["/app/bin/sample"]