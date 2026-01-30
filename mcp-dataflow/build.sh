#!/bin/bash

# Скрипт для сборки бинарников MCP DataFlow сервера для разных платформ

set -e

VERSION=${1:-"latest"}
BUILD_DIR="dist"

echo "Building MCP DataFlow Server v${VERSION}"

# Создаем директорию для бинарников
mkdir -p ${BUILD_DIR}

# Платформы для сборки
PLATFORMS=(
    "linux/amd64"
    "linux/arm64"
    "darwin/amd64"
    "darwin/arm64"
    "windows/amd64"
)

for PLATFORM in "${PLATFORMS[@]}"; do
    PLATFORM_SPLIT=(${PLATFORM//\// })
    GOOS=${PLATFORM_SPLIT[0]}
    GOARCH=${PLATFORM_SPLIT[1]}
    OUTPUT_NAME="mcp-dataflow-${GOOS}-${GOARCH}"

    if [ $GOOS = "windows" ]; then
        OUTPUT_NAME+='.exe'
    fi

    echo "Building for ${GOOS}/${GOARCH}..."
    env CGO_ENABLED=0 GOOS=${GOOS} GOARCH=${GOARCH} go build -a -installsuffix cgo -o ${BUILD_DIR}/${OUTPUT_NAME} cmd/server/main.go

    if [ $? -ne 0 ]; then
        echo "An error has occurred! Aborting the script execution..."
        exit 1
    fi
done

echo "Build complete! Binaries are in ${BUILD_DIR}/"
ls -lh ${BUILD_DIR}/
