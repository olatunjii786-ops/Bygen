FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy
WORKDIR /app
RUN pip install --no-cache-dir playwright==1.49.0 "telethon>=1.36" aiohttp tzdata
COPY bot.py .
CMD ["python", "bot.py"]
