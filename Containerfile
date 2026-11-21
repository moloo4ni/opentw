# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/opentw ./cmd/opentw

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 opentw
WORKDIR /app
COPY --from=build /out/opentw /app/opentw
COPY config.example.yaml /app/config.example.yaml
# named volume наследует владелца этой директории при первой инициализации
RUN mkdir -p /data && chown opentw:opentw /data
USER opentw
# Живость мониторинга: heartbeat обновляется в конце успешного цикла
# (и в начале цикла после успешного предыдущего). Порог приложение пишет в
# /data/opentw.heartbeat.maxage из фактического interval — образ не нужно
# пересобирать при смене конфигурации; фолбэк 2820с = maxage формулы для
# поставляемого конфига (2×15м + худший цикл + минута), стартовое покрытие —
# ниже.
#
# Худший первый цикл поставляемого конфига (config.example.yaml: 25 моделей,
# concurrency 4, http_timeout 120s): запрос каталога + ceil(25/4)=7 волн
# = (1+7)×120s = 960s — столько heartbeat может отсутствовать на живом
# старте. start-period 1200s = 960s + 25% запаса: проверки до этого
# момента не считаются провалом, ложного unhealthy нет.
# ВАЖНО: при существенно большем N/concurrency/http_timeout задайте свой
# start-period в конфигурации запуска контейнера (Quadlet: HealthStartPeriod=;
# покрыть ceil(N/K)+1 волн таймаута) — значение в образе не угадает
# произвольный конфиг автоматически.
HEALTHCHECK --interval=60s --timeout=5s --start-period=1200s --retries=3 \
  CMD test $(( $(date +%s) - $(stat -c %Y /data/opentw.heartbeat) )) -lt $(cat /data/opentw.heartbeat.maxage 2>/dev/null || echo 2820)
ENV TZ=Europe/Moscow
VOLUME ["/data"]
ENTRYPOINT ["/app/opentw"]
CMD ["-config", "/app/config.yaml"]