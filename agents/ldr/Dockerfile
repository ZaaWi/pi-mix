FROM python:3-slim
RUN pip install --no-cache-dir paho-mqtt
COPY agent.py /agent.py
CMD ["python3", "-u", "/agent.py"]