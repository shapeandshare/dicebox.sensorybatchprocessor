FROM python:2.7

WORKDIR /app

COPY ./app /app
COPY ./dicebox/lib /app/lib
COPY ./dicebox/dicebox.config /app

RUN pip install -r requirements.txt \
    && useradd -M -U -u 1000 batchprocessor \
    && chown -R batchprocessor /app

# CMD ["su", "-", "batchprocessor", "-c", "python", "./sensory_service_batch_processor.py"]
ENTRYPOINT ["python", "./sensory_service_batch_processor.py"]
CMD ["su", "-", "batchprocessor", "-c", "tail", "-f", "./logs/sensory_service_batch_processor.log"]
