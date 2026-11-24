FROM python:3.12-slim

# glibc 默认为每个线程预留 64 MiB 虚拟地址空间作为独立 malloc arena
# （HEAP_MAX_SIZE，零 RSS 但计入 RLIMIT_AS）。核查流式传输按连接占用线程，
# 多个暂停连接会把进程地址空间顶满；本服务单进程、SQLite 串行写，收敛为
# 单一 arena 即可，VmPeak 可下降数百 MiB。
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DB_PATH=/data/outage.db \
    MALLOC_ARENA_MAX=1 \
    VERIFY_TMPDIR=/data/verify-tmp

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd --create-home appuser \
    && mkdir -p /data/verify-tmp \
    && chown -R appuser:appuser /data /srv
USER appuser

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
