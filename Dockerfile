FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# 安装依赖
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 复制代码
COPY . /app

# Cloud Run 会注入 PORT
CMD ["bash", "-lc", "uvicorn spapi_probe.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
