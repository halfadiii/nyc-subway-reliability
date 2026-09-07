# The poller, containerised.
#
# Slim rather than alpine: the protobuf runtime ships manylinux wheels, and on
# musl pip falls back to building them from source, which turns a twenty-second
# image build into several minutes for no benefit.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Requirements first, so a change to the code does not invalidate the layer
# that installs the dependencies.
COPY requirements-ingest.txt ./
RUN pip install --no-cache-dir -r requirements-ingest.txt

COPY ingest/ ./ingest/

# Rows land here. Mount a volume, or set MTA_OUT to a path inside one -- a
# container that writes history to its own filesystem is a container that
# throws the history away when it restarts.
ENV MTA_OUT=/data/raw
VOLUME ["/data"]

# Runs until it is stopped. See docs/deploying.md for why this is a service
# rather than a scheduled job.
WORKDIR /app/ingest
CMD ["python", "poller.py", "--out", "/data/raw"]
