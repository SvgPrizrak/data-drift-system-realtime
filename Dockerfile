FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app

RUN groupadd --system app
RUN useradd --system --gid app --home /app app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml ./pyproject.toml
COPY --chown=app:app src ./src
RUN ruff check src
RUN ruff format --check src

USER app

EXPOSE 8000
STOPSIGNAL SIGTERM

CMD ["python", "-m", "drift_guardian.realtime.consumer"]
