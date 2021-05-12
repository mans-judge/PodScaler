FROM openjdk:8
ADD target/PodScaler-1.0-SNAPSHOT.jar PodScaler-1.0-SNAPSHOT.jar
ENTRYPOINT ["java", "-jar","PodScaler-1.0-SNAPSHOT.jar"]
EXPOSE 8080