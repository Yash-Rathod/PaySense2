FROM python:3.12-slim AS builder
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir build && python -m build --wheel

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /app/dist/*.whl .
RUN pip install --no-cache-dir *.whl

COPY src/ src/
RUN pip install --no-cache-dir -e . 2>/dev/null || true

EXPOSE 8001 9091
ENTRYPOINT ["consumer"]
