FROM python:3.11-slim

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

RUN useradd --user-group --create-home --no-log-init --shell /bin/bash pythonuser

USER pythonuser

WORKDIR /app