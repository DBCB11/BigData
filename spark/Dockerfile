# Use the base PySpark notebook image
FROM bde2020/spark-master:3.1.2-hadoop3.2


COPY spark-stream.py .
COPY script.sh .
ENV PATH=$PATH:/spark/bin
RUN pip3 install elasticsearch==7.17.0

# Set executable permission for script.sh
RUN chmod +x script.sh

ENTRYPOINT ["./script.sh"]