FROM relateiq/oracle-java7

RUN apt-get update
RUN apt-get install -y wget
RUN mkdir -p /data /opt/cassandra

RUN wget --progress=dot:mega -O - http://apache.mirrors.tds.net/cassandra/2.0.9/apache-cassandra-2.0.9-bin.tar.gz | tar -zx -C /opt/cassandra --strip-components=1

