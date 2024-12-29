FROM bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8

# Update apt sources to use a more recent Debian version (e.g., buster or bullseye)
RUN sed -i 's/stretch/buster/g' /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y git \
    && rm -rf /var/lib/apt/lists/*
