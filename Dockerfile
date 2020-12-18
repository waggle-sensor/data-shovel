FROM python:3.8-alpine
RUN pip3 install --no-cache-dir pika==1.1.*
COPY . .
ENTRYPOINT [ "python", "main.py" ]
