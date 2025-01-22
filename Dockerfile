FROM python:3.9.19-slim as builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install -r requirements.txt


# final stage
FROM python:3.9.19-slim

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app

ENV PATH="/opt/venv/bin:$PATH"

COPY ./app .

# ENTRYPOINT ["python", "save_face_metadata.py"]

# CMD ["ls -l"]

# ENTRYPOINT ["echo", "Starting this container..."]
