FROM debian:bookworm

RUN apt-get update  && mkdir -p /app/job \
    && apt-get install -y  golang wget && apt-get -y  install python3.11 python3.11-venv \
    && apt -y install default-jre

COPY starrockswriter.tar.gz /app/starrockswriter.tar.gz
COPY plugin-rdbms-util-0.0.1-SNAPSHOT.jar /app/plugin-rdbms-util-0.0.1-SNAPSHOT.jar

# wget https://github.com/StarRocks/DataX/releases/download/v1.1.5/starrockswriter.tar.gz

RUN cd /app && \
    wget https://datax-opensource.oss-cn-hangzhou.aliyuncs.com/202308/datax.tar.gz && \
    wget https://github.com/StarRocks/DataX/releases/download/v1.1.5/starrockswriter.tar.gz &&  \
    tar -zxvf datax.tar.gz && cd /app/datax/plugin/writer  \
    && tar -zxvf /app/starrockswriter.tar.gz && \
    cp /app/plugin-rdbms-util-0.0.1-SNAPSHOT.jar /app/datax/plugin/reader/mysqlreader/libs/plugin-rdbms-util-0.0.1-SNAPSHOT.jar && \
    cp /app/plugin-rdbms-util-0.0.1-SNAPSHOT.jar /app/datax/plugin/reader/rdbmsreader/libs/plugin-rdbms-util-0.0.1-SNAPSHOT.jar

WORKDIR /app
COPY . .
ENV GOMODCACHE /tmp/.cache
ENV GOCACHE /tmp/.cache
RUN go build -o /app/woo-datax

CMD ["/app/woo-datax"]