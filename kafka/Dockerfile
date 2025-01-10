FROM python:3.10

COPY . ./kafka
RUN apt-get update && apt-get install -y netcat-openbsd
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r ./kafka/requirement.txt

# Default command to run Python
	
ENTRYPOINT ["/kafka/script.sh"]


