FROM debian:bookworm-slim
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      xvfb chromium ffmpeg python3 python3-websockets socat \
      fonts-dejavu-core fonts-noto-core fonts-noto-color-emoji fonts-liberation ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /film
