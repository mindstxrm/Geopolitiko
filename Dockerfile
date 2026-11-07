FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.lock ./
RUN pip install --upgrade pip \
 && if [ -f requirements.lock ]; then pip install -r requirements.lock; \
    else pip install -r requirements.txt; fi

COPY . .

RUN mkdir -p /app/data /app/logs /app/data/backups

EXPOSE 5003 5005

CMD ["gunicorn", "-b", "0.0.0.0:5003", "-w", "2", "--timeout", "120", "run:app"]
