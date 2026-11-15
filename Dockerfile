FROM python:3.11-slim
WORKDIR /app
COPY nexmark_join /app/nexmark_join
COPY function/handler.py /app/handler.py
ENV PYTHONUNBUFFERED=1
ENV NEXMARK_BACKEND=memory
EXPOSE 8080
CMD ["python", "-u", "/app/handler.py"]
