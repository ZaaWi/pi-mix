FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    netcat-openbsd \
    tar \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://github.com/arduino/arduino-cli/releases/download/v1.5.1/arduino-cli_1.5.1_Linux_ARM64.tar.gz \
    | tar xz -C /usr/local/bin

RUN arduino-cli core install arduino:avr
RUN arduino-cli lib install "DHT sensor library"
COPY dht11-ldr /firmware/dht11-ldr
COPY flash.sh /flash.sh
CMD ["/flash.sh"]
