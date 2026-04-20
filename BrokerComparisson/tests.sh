#!/bin/bash

OUTPUT_FILE="benchmark_results.txt"
echo "=== СТАРТ BENCHMARK: $(date) ===" > $OUTPUT_FILE

go build -o consumer_bin cmd/consumer/main.go
go build -o producer_bin cmd/producer/main.go

run_test() {
    BROKER=$1
    SIZE=$2
    COUNT=$3
    WORKERS=$4
    RATE=$5

    echo "=====================================================" | tee -a $OUTPUT_FILE
    echo "ТЕСТ: [$BROKER] | Размер: $SIZE | Кол-во: $COUNT | Воркеры: $WORKERS | Лимит RPS: ${RATE:-Нет}" | tee -a $OUTPUT_FILE
    echo "=====================================================" | tee -a $OUTPUT_FILE

    # Запускаем консьюмера в фоне и сохраняем его PID
    ./consumer_bin -type $BROKER > temp_cons.log 2>&1 &
    CONS_PID=$!
    
    # Даем консьюмеру секунду на подключение к брокеру
    sleep 2 

    # Запускаем продюсера
    if [ -z "$RATE" ]; then
        ./producer_bin -type $BROKER -size $SIZE -count $COUNT -workers $WORKERS | tee -a $OUTPUT_FILE
    else
        ./producer_bin -type $BROKER -size $SIZE -count $COUNT -workers $WORKERS -rate $RATE | tee -a $OUTPUT_FILE
    fi

    # Даем консьюмеру время вычитать остатки сообщений из очереди
    sleep 3 

    # Отправляем SIGINT (Ctrl+C) консьюмеру, чтобы он распечатал таблицу stats
    kill -SIGINT $CONS_PID
    sleep 1

    # Переносим вывод консьюмера в общий лог
    cat temp_cons.log >> $OUTPUT_FILE
    echo -e "\n\n" >> $OUTPUT_FILE
}

echo "Запуск тестов..."

# --- СЕРИЯ 1: Максимальная пропускная способность (Микро-сообщения) ---
run_test "rabbit" 128 100000 20
run_test "redis" 128 100000 20

# --- СЕРИЯ 2: Стандартный Payload ---
run_test "rabbit" 1024 50000 20
run_test "redis" 1024 50000 20

# --- СЕРИЯ 3: Тяжелые сообщения (Стресс по памяти) ---
run_test "rabbit" 102400 10000 10
run_test "redis" 102400 10000 10

# --- СЕРИЯ 4: Интенсивность (Стабильный поток, замер чистой Latency) ---
run_test "rabbit" 1024 50000 20 5000
run_test "redis" 1024 50000 20 5000

# --- СЕРИЯ 5: Высокая конкурентность (Много соединений/каналов) ---
run_test "rabbit" 512 50000 100
run_test "redis" 512 50000 100

# Очистка следов
rm temp_cons.log consumer_bin producer_bin

echo "Тесты завершены! Все данные лежат в файле $OUTPUT_FILE"