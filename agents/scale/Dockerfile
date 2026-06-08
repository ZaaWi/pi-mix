# Uses bleak (BlueZ D-Bus API) instead of btmon because:
#   btmon opens HCI monitor sockets (AF_BLUETOOTH, SOCK_RAW, HCI_CHANNEL_MONITOR).
#   Even with privileged:true, socket(AF_BLUETOOTH) returns EAFNOSUPPORT in
#   k3s containers (kernel module loaded on host but socket family unavailable
#   from container netns). bleak communicates via D-Bus with host bluetoothd,
#   which works since /run/dbus is mounted.
FROM python:3-slim
RUN pip install --no-cache-dir bleak paho-mqtt
COPY agent.py /agent.py
CMD ["python3", "-u", "/agent.py"]
