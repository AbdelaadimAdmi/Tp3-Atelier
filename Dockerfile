FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY . .
RUN mvn -B -ntp -DskipTests package

FROM eclipse-temurin:21-jre
ARG SERVICE
WORKDIR /app
COPY --from=build /build/${SERVICE}/target/${SERVICE}-1.0.0.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=65", "-XX:ActiveProcessorCount=2", "-jar", "/app/app.jar"]
