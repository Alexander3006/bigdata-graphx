FROM docker.io/bitnami/spark:3.3

USER root
RUN install_packages unzip
RUN install_packages wget

RUN wget https://neo4j.com/artifact.php?name=neo4j-connector-apache-spark-5.0.0.zip
RUN unzip artifact.php?name=neo4j-connector-apache-spark-5.0.0.zip -d /opt/bitnami/spark/jars/
RUN rm artifact.php?name=neo4j-connector-apache-spark-5.0.0.zip