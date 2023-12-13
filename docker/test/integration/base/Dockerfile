# rebuild in #33610
# docker build -t clickhouse/integration-test .
ARG FROM_TAG=latest
FROM altinityinfra/test-base:$FROM_TAG

SHELL ["/bin/bash", "-c"]

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get -y install \
        bsdutils=1:2.37.2-4ubuntu3 \
        curl='7.81.*' \
        default-jre=2:1.11-72build2 \
        g++=4:11.2.0-1ubuntu1 \
        gdb=12.1-0ubuntu1~22.04 \
        iproute2=5.15.0-1ubuntu2 \
        krb5-user=1.19.2-2ubuntu0.3 \
        libicu-dev=70.1-2 \
        libsqlite3-dev=3.37.2-2ubuntu0.1 \
        libsqliteodbc=0.9998-3 \
        lsof=4.93.2+dfsg-1.1build2 \
        lz4=1.9.3-2build2 \
        odbc-postgresql=1:13.02.0000-2 \
        odbcinst=2.3.9-5 \
        python3=3.10.6-1~22.04 \
        rpm2cpio=4.17.0+dfsg1-4build1 \
        sqlite3=3.37.2-2ubuntu0.1 \
        tar=1.34+dfsg-1ubuntu0.1.22.04.1 \
        tzdata=2023c-0ubuntu0.22.04.2 \
        unixodbc=2.3.9-5 \
        python3-pip=22.0.2+dfsg-1ubuntu0.4 \
        libcurl4-openssl-dev='7.81.*' \
        libssl-dev=3.0.2-0ubuntu1.12 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

RUN pip3 install pycurl==7.45.2

# Architecture of the image when BuildKit/buildx is used
ARG TARGETARCH

# Install MySQL ODBC driver from RHEL rpm
# For reference https://downloads.mysql.com/archives/c-odbc/ RHEL
RUN arch=${TARGETARCH:-amd64} \
  && case $arch in \
      amd64) rarch=x86_64 ;; \
      arm64) rarch=aarch64 ;; \
    esac \
  && cd /tmp \
  && curl -o mysql-odbc.rpm "https://cdn.mysql.com/archives/mysql-connector-odbc-8.0/mysql-connector-odbc-8.0.32-1.el9.${rarch}.rpm" \
  && rpm2archive mysql-odbc.rpm \
  && tar xf mysql-odbc.rpm.tgz -C / ./usr/lib64/ \
  && rm mysql-odbc.rpm mysql-odbc.rpm.tgz \
  && ODBC_DIR=$(dpkg -L odbc-postgresql | rg '^/usr/lib/.*-linux-gnu/odbc$') \
  && ln -s /usr/lib64/libmyodbc8a.so "$ODBC_DIR" \
  && ln -s /usr/lib64/libmyodbc8a.so "$ODBC_DIR"/libmyodbc.so

# Unfortunately this is required for a single test for conversion data from zookeeper to clickhouse-keeper.
# ZooKeeper is not started by default, but consumes some space in containers.
# 777 perms used to allow anybody to start/stop ZooKeeper
ENV ZOOKEEPER_VERSION='3.6.3'
RUN curl "https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz" | \
      tar -C opt -zxv && \
  mv /opt/apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper && \
  chmod -R 777 /opt/zookeeper && \
  echo $'tickTime=2500 \n\
tickTime=2500 \n\
dataDir=/zookeeper \n\
clientPort=2181 \n\
maxClientCnxns=80' > /opt/zookeeper/conf/zoo.cfg && \
  mkdir /zookeeper && \
  chmod -R 777 /zookeeper

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
