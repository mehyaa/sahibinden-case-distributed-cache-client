FROM gradle:9.1-jdk17 AS builder

WORKDIR /src

COPY . .

RUN gradle :cache-server:clean :cache-server:installDist -x test --no-daemon

FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY --from=builder /src/cache-server/build/install/cache-server /app

EXPOSE 6379

ENTRYPOINT ["/app/bin/cache-server"]