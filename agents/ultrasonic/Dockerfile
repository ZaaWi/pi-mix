# Uses debian:trixie-slim (not python:3-slim) because:
#   python:3-slim now ships Python 3.14, but apt package python3-libgpiod
#   installs C extensions compiled for system Python 3.13 — ABI mismatch.
#   debian:trixie-slim's system Python (3.13) matches, so import gpiod works.
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y python3 python3-libgpiod python3-paho-mqtt && rm -rf /var/lib/apt/lists/*
COPY agent.py /agent.py
CMD ["python3", "-u", "/agent.py"]
