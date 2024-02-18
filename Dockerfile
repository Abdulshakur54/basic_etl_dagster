FROM ubuntu

WORKDIR /app

RUN apt-get update && \
    apt-get install -y python3 python3-pip

RUN pip install dagster-webserver==1.5.13

COPY image_requirements.txt .
    
RUN pip install -r image_requirements.txt

COPY . .

ENV DAGSTER_HOME=/app

ENTRYPOINT ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]

EXPOSE 3000


