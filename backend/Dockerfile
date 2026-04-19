FROM python:3.12-slim

WORKDIR /backend

RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./backend/
WORKDIR /backend/backend

EXPOSE 8000

CMD sh -c "python generate_data.py && uvicorn main:app --host 0.0.0.0 --port $BACKEND_PORT --reload"