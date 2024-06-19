FROM python:3.11-slim-buster
RUN apt update -y && apt install -y awscli build-essential
WORKDIR /app

COPY . /app
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "main.py"]