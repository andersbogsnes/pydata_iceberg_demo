FROM python:3.12-slim

RUN apt-get update -y && apt-get install -y jq

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

RUN useradd --user-group --create-home --no-log-init --shell /bin/bash pythonuser

USER pythonuser

WORKDIR /app