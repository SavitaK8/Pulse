FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY *.java ./
RUN mkdir /out && javac -encoding UTF-8 -d /out *.java

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /out /app
EXPOSE 9001
USER 10001
ENTRYPOINT ["java", "PulseNode"]
