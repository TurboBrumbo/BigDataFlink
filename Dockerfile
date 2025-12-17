FROM flink:1.18.1-scala_2.12-java17

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev build-essential && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    apache-flink==1.18.1 \
    apache-beam==2.48.0 \
    "protobuf<5" \
    grpcio \
    psycopg2-binary \
    kafka-python

USER flink
