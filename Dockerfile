FROM python:3.11-slim

# Зависимости для Pillow (JPEG/PNG/TIFF/WEBP)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libjpeg62-turbo-dev \
    zlib1g-dev \
    libpng-dev \
    libwebp-dev \
    libtiff-dev \
    libopenjp2-7-dev \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# опционально ненулевой пользователь
RUN useradd -m runner && chown -R runner:runner /app
USER runner

# Значения по умолчанию (будут переопределены из docker-compose.yml при запуске)
ENV OUT_DIR=/app/images \
    DB_HOST=mysql \
    DB_PORT=3306 \
    DB_USER=root \
    DB_PASSWORD=root \
    DB_NAME=domus_LOCAL \
    DB_TABLE_PRODUCTS=products

RUN mkdir -p ${OUT_DIR}

ENTRYPOINT ["python", "download_images.py"]
