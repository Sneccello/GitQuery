FROM python:3.12-rc-buster

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*


COPY streamlit_app/requirements.txt /app/requirements.txt

WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501

ENV HDFS_URL="http://namenode:9870"

# Set HDFS configuration (e.g., to point to the Hadoop namenode)
RUN echo "fs.defaultFS=${HDFS_NAMENODE_URI}" > $HADOOP_CONF_DIR/core-site.xml \
    && echo "<configuration>" >> $HADOOP_CONF_DIR/core-site.xml \
    && echo "  <property>" >> $HADOOP_CONF_DIR/core-site.xml \
    && echo "    <name>fs.defaultFS</name>" >> $HADOOP_CONF_DIR/core-site.xml \
    && echo "    <value>${HDFS_NAMENODE_URI}</value>" >> $HADOOP_CONF_DIR/core-site.xml \
    && echo "  </property>" >> $HADOOP_CONF_DIR/core-site.xml \
    && echo "</configuration>" >> $HADOOP_CONF_DIR/core-site.xml

CMD ["streamlit", "run", "About.py"]
