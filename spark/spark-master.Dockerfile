# Use the base PySpark notebook image
FROM bde2020/spark-master:3.1.2-hadoop3.2


# Switch to root to install new Python version
USER root

# Update package list and install required tools including curl and Python 3.7
RUN apt-get update && \
    apt-get install -y software-properties-common curl && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.9 python3.9-distutils python3.9-dev && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3.9 1 && \
    update-alternatives --set python /usr/bin/python3.9 && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python

# Clean up unnecessary packages
RUN apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/*

# Verify Python version
RUN python --version

# Switch back to the notebook user
USER ${NB_UID}