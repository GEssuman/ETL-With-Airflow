FROM astrocrpublic.azurecr.io/runtime:3.0-2

USER root

# Install Java 11 and wget
RUN apt-get update && \
    apt-get install -y wget curl gnupg2 software-properties-common && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Set environment variables for Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:$PATH"

USER astro
