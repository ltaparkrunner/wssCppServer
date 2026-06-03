# === ЭТАП 1: Сборка зависимостей и сервера ===
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

RUN H="https://github.com" && R="microsoft/vcpkg/archive/refs/heads/master.zip" && curl -L "${H}/${R}" -o vcpkg.zip \
    && unzip vcpkg.zip && mv vcpkg-master vcpkg && rm vcpkg.zip

RUN ./vcpkg/bootstrap-vcpkg.sh -disableMetrics
ENV VVCPKG_ROOT=/opt/vcpkg

# ИСПРАВЛЕНО ТУТ: Убран несуществующий libbcrypt, оставлен только чистый рабочий стек
RUN /opt/vcpkg/vcpkg install \
    boost-asio \
    boost-beast \
    boost-thread \
    protobuf \
    nlohmann-json \
    jwt-cpp \
    mongo-cxx-driver \
    aws-sdk-cpp[s3] \
    --triplet x64-linux

WORKDIR /build_app
COPY CMakeLists.txt image.proto ./
COPY src/ ./src/

RUN cmake -G Ninja -B build -S . \
    -DCMAKE_MAKE_PROGRAM=/usr/bin/ninja \
    -DCMAKE_C_COMPILER=/usr/bin/gcc \
    -DCMAKE_CXX_COMPILER=/usr/bin/g++ \
    -DCMAKE_TOOLCHAIN_FILE=${VVCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake \
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

COPY --from=builder /build_app/dist/picture_server .
COPY --from=builder /opt/vcpkg/packages/*_x64-linux/lib/*.so* /usr/local/lib/
RUN ldconfig

COPY cert.pem key.pem ./

EXPOSE 8080
EXPOSE 8081

CMD ["./picture_server"]
