# rebuild in #33610
# docker build -t clickhouse/stateless-test .
ARG FROM_TAG=latest
FROM altinityinfra/test-base:$FROM_TAG

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.4.20200302/clickhouse-odbc-1.1.4-Linux.tar.gz"

# golang version 1.13 on Ubuntu 20 is enough for tests
RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            awscli=1.22.34-1 \
            brotli=1.0.9-2build6 \
            lz4=1.9.3-2build2 \
            expect=5.45.4-2build1 \
            golang=2:1.18~0ubuntu2 \
            lsof=4.93.2+dfsg-1.1build2 \
            mysql-client=8.0* \
            ncdu=1.15.1-1 \
            netcat-openbsd=1.218-4ubuntu1 \
            nodejs=12.22.9~dfsg-1ubuntu3.2 \
            npm=8.5.1~ds-1 \
            odbcinst=2.3.9-5 \
            openjdk-11-jre-headless=11.0.21+9-0ubuntu1~22.04 \
            openssl=3.0.2-0ubuntu1.12 \
            postgresql-client=14+238 \
            protobuf-compiler=3.12.4-1ubuntu7.22.04.1 \
            python3=3.10.6-1~22.04 \
            python3-lxml=4.8.0-1build1 \
            python3-pip=22.0.2+dfsg-1ubuntu0.4 \
            python3-requests=2.25.1+dfsg-2ubuntu0.1 \
            python3-termcolor=1.1.0-3 \
            qemu-user-static=1:6.2+dfsg-2ubuntu6.15 \
            sqlite3=3.37.2-2ubuntu0.1 \
            sudo=1.9.9-1ubuntu2.4 \
            tree=2.0.2-1 \
            unixodbc=2.3.9-5 \
            wget=1.21.2-2ubuntu1 \
            rustc=1.70.0+dfsg0ubuntu1~bpo2-0ubuntu0.22.04.2 \
            cargo=1.70.0+dfsg0ubuntu1~bpo2-0ubuntu0.22.04.2 \
            zstd=1.4.8+dfsg-3build1 \
            file=1:5.41-3ubuntu0.1 \
            pv=1.6.6-1build2 \
            zip=3.0-12build2 \
            p7zip-full=16.02+dfsg-8 \
            rpm2cpio=4.17.0+dfsg1-4build1 \
            cpio=2.13+dfsg-7 \
    && apt-get clean

RUN pip3 install numpy==1.26.3 scipy==1.12.0 pandas==1.5.3 Jinja2==3.1.3

RUN mkdir -p /tmp/clickhouse-odbc-tmp \
   && wget -nv -O - ${odbc_driver_url} | tar --strip-components=1 -xz -C /tmp/clickhouse-odbc-tmp \
   && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib/ \
   && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
   && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
   && rm -rf /tmp/clickhouse-odbc-tmp

ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV NUM_TRIES=1
ENV MAX_RUN_TIME=0

# Unrelated to vars in setup_minio.sh, but should be the same there
# to have the same binaries for local running scenario
ARG MINIO_SERVER_VERSION=2022-01-03T18-22-58Z
ARG MINIO_CLIENT_VERSION=2022-01-05T23-52-51Z
ARG TARGETARCH

# Download Minio-related binaries
RUN arch=${TARGETARCH:-amd64} \
    && wget "https://dl.min.io/server/minio/release/linux-${arch}/archive/minio.RELEASE.${MINIO_SERVER_VERSION}" -O ./minio \
    && wget "https://dl.min.io/client/mc/release/linux-${arch}/archive/mc.RELEASE.${MINIO_CLIENT_VERSION}" -O ./mc \
    && chmod +x ./mc ./minio


RUN wget --no-verbose 'https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz' \
    && tar -xvf hadoop-3.3.1.tar.gz \
    && rm -rf hadoop-3.3.1.tar.gz

ENV MINIO_ROOT_USER="clickhouse"
ENV MINIO_ROOT_PASSWORD="clickhouse"
ENV EXPORT_S3_STORAGE_POLICIES=1

RUN npm install -g azurite@3.28.0 \
    && npm install -g tslib@2.6.2

COPY run.sh /
COPY setup_minio.sh /
COPY setup_hdfs_minicluster.sh /

CMD ["/bin/bash", "/run.sh"]
