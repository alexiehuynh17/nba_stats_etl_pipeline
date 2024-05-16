FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update 
RUN apt-get install -y --no-install-recommends openjdk-17-jre-headless
RUN apt-get autoremove -yqq --purge
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ADD requirements.txt .  

RUN pip install -r requirements.txt