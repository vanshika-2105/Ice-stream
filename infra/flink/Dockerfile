FROM flink:1.20.1-scala_2.12-java17

RUN apt-get update \
    && apt-get install -y python3 python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir apache-flink==1.20.1
