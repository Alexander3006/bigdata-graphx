FROM jupyter/base-notebook:python-3.8

USER root
WORKDIR /home/alex

RUN sudo apt-get update
RUN sudo apt-get install default-jdk -y

RUN sudo apt install python3-pip -y
RUN pip install pyspark