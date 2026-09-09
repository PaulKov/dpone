FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    build-essential \
    git \
  && rm -rf /var/lib/apt/lists/*

# Ставим uv в глобальный путь (доступен любому пользователю в контейнере)
ENV UV_INSTALL_DIR=/usr/local/bin

ADD https://astral.sh/uv/install.sh /tmp/uv-installer.sh
RUN sh /tmp/uv-installer.sh \
  && rm -f /tmp/uv-installer.sh \
  && uv --version \
  && python --version

WORKDIR /app

# (опционально) директория под кэш uv — удобно для GitLab cache
ENV UV_CACHE_DIR=/cache/uv
RUN mkdir -p /cache/uv && chmod -R 777 /cache/uv
