FROM apache/superset:latest

USER root

RUN pip install psycopg2-binary

ENV ADMIN_USERNAME $ADMIN_USERNAME
ENV ADMIN_EMAIL $ADMIN_EMAIL
ENV ADMIN_PASSWORD $ADMIN_PASSWORD

COPY ./superset-init.sh /superset-init.sh

COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

CMD ["chmod", "-R","777","/superset-init.sh"]

USER superset
ENTRYPOINT [ "bash", "/superset-init.sh" ]