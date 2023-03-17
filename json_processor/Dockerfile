FROM marketplace.gcr.io/google/ubuntu2004:latest

WORKDIR /usr/app/src

COPY batch_processor.py ./
RUN set -xe \
    && apt-get update -y \
    && apt-get install python3-pip -y

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir google-cloud-storage ijson google-cloud-bigquery gcsfs

CMD ["python3","-u","batch_processor.py"]
