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

# La version est celle du tag de l image, passee par la CI. Elle est a la fois un label
# (l image se decrit elle-meme) et une variable d environnement (les routes la publient dans le
# lifesign : CAMEL_WORKER_VERSION). Le compose peut la surcharger.
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.title="Camel4Container" \
      org.opencontainers.image.description="Camel 4 runner: charge un camel-context.xml externe" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.source="https://github.com/snuids/Camel4Container" \
      org.opencontainers.image.licenses="GPL-3.0-only"
ENV CAMEL_WORKER_VERSION="${VERSION}"

# Set the working directory
WORKDIR /app

# Copy the built jar from the build stage
COPY --from=build /app/target/app.jar app.jar
# RUN mkdir lib
COPY --from=build /app/target/lib/* lib/
COPY start.sh .

COPY src/main/log4j2.xml .
COPY data/camel-context.xml .
RUN chmod +x ./start.sh
RUN ls -l

# # Command to run the application
CMD ["./start.sh"]