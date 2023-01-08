FROM jupyter/base-notebook:python-3.8

USER root
WORKDIR /home/alex

RUN sudo apt-get update
RUN sudo apt-get install default-jdk -y

RUN sudo apt install python3-pip -y
RUN pip install pyspark
RUN pip install graphframes

RUN sudo apt-get install unzip
RUN sudo apt-get install wget

RUN wget https://neo4j.com/artifact.php?name=neo4j-connector-apache-spark-5.0.0.zip
RUN unzip artifact.php?name=neo4j-connector-apache-spark-5.0.0.zip -d ./neo4j
RUN rm artifact.php?name=neo4j-connector-apache-spark-5.0.0.zip

RUN mkdir -p /root/.m2/repository/neo4j-contrib/neo4j-connector-apache-spark_2.13/5.0.0_for_spark_3/
RUN cp -a ./neo4j/. /root/.m2/repository/neo4j-contrib/neo4j-connector-apache-spark_2.13/5.0.0_for_spark_3/
RUN cp -a ./neo4j/. /home/alex/