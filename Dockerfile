#FROM maven:3.8.6-openjdk-23-slim AS build
FROM maven:3.9.9-amazoncorretto-23-alpine AS build

# Set the working directory
#WORKDIR /app
#RUN ls -l
#RUN pwd
# Copy the pom.xml and source code
#COPY pom.xml .

#apt update
#apt install -y jq

WORKDIR /app
COPY pom.xml .
COPY src ./src
# Build the project
RUN mvn clean install


# Use a smaller base image for the final image
FROM openjdk:25-jdk-slim

# Set the working directory
WORKDIR /app

# Copy the built jar from the build stage
COPY --from=build /app/target/camel-1.0-SNAPSHOT.jar app.jar
# RUN mkdir lib
COPY --from=build /app/target/lib/* lib/
COPY start.sh .

COPY src/main/log4j2.xml .
COPY data/camel-context.xml .
RUN chmod +x ./start.sh
RUN ls -l

# # Command to run the application
CMD ["./start.sh"]