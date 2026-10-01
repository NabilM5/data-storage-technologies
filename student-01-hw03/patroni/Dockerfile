FROM postgres:16.9

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl python3 python3-pip \
    && pip3 install --break-system-packages --no-cache-dir "patroni[etcd3]==4.1.5" "psycopg[binary]>=3.1" \
    && rm -rf /var/lib/apt/lists/*

USER postgres
