#!/bin/bash

# Скрипт для запуска тестов с выводом статистики
# Использование: ./scripts/run-tests-with-stats.sh [путь к тестам]
# Пример: ./scripts/run-tests-with-stats.sh ./test/integration/...

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Путь к тестам (по умолчанию все тесты)
TEST_PATH="${1:-./...}"
# Опциональные флаги (например GO_TEST_TAGS=integration для тестов с контейнерами)
EXTRA_GO_FLAGS=""
if [ -n "${GO_TEST_TAGS:-}" ]; then
  EXTRA_GO_FLAGS="-tags=${GO_TEST_TAGS}"
fi

echo -e "${BLUE}=== Запуск тестов: ${TEST_PATH} ===${NC}\n"

# Временный файл для JSON вывода
TEMP_JSON=$(mktemp)
TEMP_OUTPUT=$(mktemp)

# Запускаем тесты с JSON выводом
set +e
go test ${EXTRA_GO_FLAGS} -json "${TEST_PATH}" -v 2>&1 | tee "${TEMP_JSON}" > "${TEMP_OUTPUT}"
TEST_EXIT_CODE=$?
set -e

# Парсим JSON и собираем статистику
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
SKIPPED_TESTS=0
declare -A FAILED_MAP

# Используем jq если доступен
if command -v jq &> /dev/null; then
    # Парсим через jq - более надежно
    TOTAL_TESTS=$(jq -r 'select(.Action=="run" and .Test != null) | "\(.Package).\(.Test)"' "${TEMP_JSON}" 2>/dev/null | sort -u | wc -l | tr -d ' ')
    PASSED_TESTS=$(jq -r 'select(.Action=="pass" and .Test != null) | "\(.Package).\(.Test)"' "${TEMP_JSON}" 2>/dev/null | sort -u | wc -l | tr -d ' ')
    FAILED_TESTS=$(jq -r 'select(.Action=="fail" and .Test != null) | "\(.Package).\(.Test)"' "${TEMP_JSON}" 2>/dev/null | sort -u | wc -l | tr -d ' ')
    SKIPPED_TESTS=$(jq -r 'select(.Action=="skip" and .Test != null) | "\(.Package).\(.Test)"' "${TEMP_JSON}" 2>/dev/null | sort -u | wc -l | tr -d ' ')

    # Собираем список упавших тестов
    while IFS= read -r failed_test; do
        if [ -n "$failed_test" ] && [ "$failed_test" != "null" ] && [ "$failed_test" != "." ]; then
            FAILED_MAP["$failed_test"]=1
        fi
    done < <(jq -r 'select(.Action=="fail" and .Test != null) | "\(.Package).\(.Test)"' "${TEMP_JSON}" 2>/dev/null | sort -u)
else
    # Простой парсинг без jq - используем grep и awk
    while IFS= read -r line; do
        # Парсим JSON строки
        if echo "$line" | grep -q '"Action":"run"'; then
            if echo "$line" | grep -q '"Test":'; then
                TOTAL_TESTS=$((TOTAL_TESTS + 1))
            fi
        elif echo "$line" | grep -q '"Action":"pass"'; then
            if echo "$line" | grep -q '"Test":'; then
                PASSED_TESTS=$((PASSED_TESTS + 1))
            fi
        elif echo "$line" | grep -q '"Action":"fail"'; then
            if echo "$line" | grep -q '"Test":'; then
                FAILED_TESTS=$((FAILED_TESTS + 1))
                # Извлекаем Package и Test
                PACKAGE=$(echo "$line" | grep -o '"Package":"[^"]*"' | head -1 | cut -d'"' -f4)
                TEST=$(echo "$line" | grep -o '"Test":"[^"]*"' | head -1 | cut -d'"' -f4)
                if [ -n "$PACKAGE" ] && [ -n "$TEST" ] && [ "$PACKAGE" != "null" ] && [ "$TEST" != "null" ]; then
                    FAILED_MAP["${PACKAGE}.${TEST}"]=1
                fi
            fi
        elif echo "$line" | grep -q '"Action":"skip"'; then
            if echo "$line" | grep -q '"Test":'; then
                SKIPPED_TESTS=$((SKIPPED_TESTS + 1))
            fi
        fi
    done < "${TEMP_JSON}"

    # Если не получилось из JSON, парсим обычный вывод
    if [ "${TOTAL_TESTS}" -eq 0 ]; then
        TOTAL_TESTS=$(grep -c "^=== RUN" "${TEMP_OUTPUT}" 2>/dev/null || echo "0")
        PASSED_TESTS=$(grep -c "^--- PASS" "${TEMP_OUTPUT}" 2>/dev/null || echo "0")
        FAILED_TESTS=$(grep -c "^--- FAIL" "${TEMP_OUTPUT}" 2>/dev/null || echo "0")
        SKIPPED_TESTS=$(grep -c "^--- SKIP" "${TEMP_OUTPUT}" 2>/dev/null || echo "0")

        # Собираем упавшие тесты из обычного вывода
        while IFS= read -r line; do
            if echo "$line" | grep -q "^--- FAIL:"; then
                test_name=$(echo "$line" | sed 's/^--- FAIL: //' | awk '{print $1}')
                if [ -n "$test_name" ]; then
                    # Пытаемся найти package из предыдущих строк
                    package=$(grep -B 10 "^--- FAIL: $test_name" "${TEMP_OUTPUT}" | grep "^ok\|^FAIL" | tail -1 | awk '{print $NF}' | sed 's|^\./||' | sed 's|/|.|g' || echo "")
                    if [ -z "$package" ]; then
                        package=$(grep -B 5 "^--- FAIL: $test_name" "${TEMP_OUTPUT}" | grep "^\s" | tail -1 | awk '{print $1}' | xargs dirname | sed 's|^\./||' | sed 's|/|.|g' || echo "")
                    fi
                    if [ -n "$package" ] && [ -n "$test_name" ]; then
                        FAILED_MAP["${package}.${test_name}"]=1
                    elif [ -n "$test_name" ]; then
                        FAILED_MAP["${test_name}"]=1
                    fi
                fi
            fi
        done < "${TEMP_OUTPUT}"
    fi
fi

# Преобразуем map в массив для вывода
FAILED_LIST=()
for key in "${!FAILED_MAP[@]}"; do
    FAILED_LIST+=("$key")
done

# Сортируем список
IFS=$'\n' FAILED_LIST=($(sort <<<"${FAILED_LIST[*]}"))
unset IFS

# Убеждаемся, что все переменные имеют значения
TOTAL_TESTS=${TOTAL_TESTS:-0}
PASSED_TESTS=${PASSED_TESTS:-0}
FAILED_TESTS=${FAILED_TESTS:-0}
SKIPPED_TESTS=${SKIPPED_TESTS:-0}

# Выводим статистику
echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}                    СТАТИСТИКА ТЕСТОВ${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""
printf "%-20s ${BLUE}%d${NC}\n" "Всего тестов:" "${TOTAL_TESTS}"
printf "%-20s ${GREEN}%d${NC}\n" "Успешно:" "${PASSED_TESTS}"
printf "%-20s ${RED}%d${NC}\n" "Провалено:" "${FAILED_TESTS}"
printf "%-20s ${YELLOW}%d${NC}\n" "Пропущено:" "${SKIPPED_TESTS}"
echo ""

# Выводим список упавших тестов, если есть
if [ ${#FAILED_LIST[@]} -gt 0 ]; then
    echo -e "${RED}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${RED}              СПИСОК ПРОВАЛЕННЫХ ТЕСТОВ${NC}"
    echo -e "${RED}═══════════════════════════════════════════════════════════${NC}"
    echo ""
    for failed_test in "${FAILED_LIST[@]}"; do
        echo -e "${RED}  ✗ ${failed_test}${NC}"
    done
    echo ""
fi

# Выводим итоговый результат
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
if [ ${#FAILED_LIST[@]} -eq 0 ] && [ ${TEST_EXIT_CODE} -eq 0 ]; then
    echo -e "${GREEN}                    ВСЕ ТЕСТЫ ПРОЙДЕНЫ ✓${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
    rm -f "${TEMP_JSON}" "${TEMP_OUTPUT}"
    exit 0
else
    echo -e "${RED}                    ЕСТЬ ПРОВАЛЕННЫЕ ТЕСТЫ ✗${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
    rm -f "${TEMP_JSON}" "${TEMP_OUTPUT}"
    exit 1
fi
