# ─── Build stage ──────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Системные зависимости для asyncpg (нужен gcc)
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --prefix=/install -r requirements.txt

# ─── Runtime stage ────────────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app

# Копируем установленные пакеты из builder
COPY --from=builder /install /usr/local

# Копируем исходники
COPY main.py .
COPY index.html .

EXPOSE 8000

CMD ["python", "main.py"]
