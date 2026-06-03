# === ЭТАП 1: Сборка через vcpkg и CMake ===
FROM ubuntu:24.04 AS builder
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    git \
    curl \
    zip \
    unzip \
    tar \
    pkg-config \
    ca-certificates \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt

# Скачиваем vcpkg безопасным curl-методом
RUN H="https://github.com" && R="microsoft/vcpkg/archive/refs/heads/master.zip" && curl -L "${H}/${R}" -o vcpkg.zip
RUN unzip vcpkg.zip && mv vcpkg-master vcpkg && rm vcpkg.zip

RUN ./vcpkg/bootstrap-vcpkg.sh -disableMetrics
ENV VCPKG_ROOT=/opt/vcpkg

# Копируем проект и манифест зависимостей
WORKDIR /build_app
COPY vcpkg.json CMakeLists.txt image.proto ./
COPY src/ ./src/

# Гарантированное исправление: заменяем устаревшую команду прямо внутри контейнера,
# чтобы обойти любые проблемы с кэшированием слоев Docker
# RUN sed -i 's/protobuf_generate_cpp/protobuf_generate/g' CMakeLists.txt

# Конфигурируем и собираем с флагом INSTALL
RUN cmake -G Ninja -B build -S . \
    -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/build_app/dist
    
RUN cmake --build build --config Release --target install

# === ЭТАП 2: Финальный легковесный рантайм-контейнер ===
FROM ubuntu:24.04
WORKDIR /app

RUN apt-get update && apt-get install -y \
    libssl3 \
    libcurl4 \
    && rm -rf /var/lib/apt/lists/*

# Забираем бинарник из папки dist
COPY --from=builder /build_app/dist/picture_server .

# Копируем автоматически собранные vcpkg динамические библиотеки (.so)
COPY --from=builder /build_app/build/vcpkg_installed/x64-linux/lib/*.so* /usr/local/lib/
RUN ldconfig

# Копируем SSL-сертификаты
COPY cert.pem key.pem ./

EXPOSE 8080
EXPOSE 8081

CMD ["./picture_server"]
