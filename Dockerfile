FROM neo4j:5.14-enterprise
COPY --chown=neo4j:neo4j plugins/* /plugins/