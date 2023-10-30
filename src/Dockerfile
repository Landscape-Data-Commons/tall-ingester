# python ubuntu image
FROM python:3.8.12-slim-buster

RUN echo "America/Denver" > /etc/timezone
RUN dpkg-reconfigure -f noninteractive tzdata

# install linux dependencies
RUN apt-get update && \
    apt-get install postgresql \
            postgresql-contrib \
            unixodbc-dev \
            libpq-dev \
            g++ \
            curl \
            default-jre \
            bash -y && \
            pip3 install --upgrade pip

# prepare environment
WORKDIR /usr/src
#RUN mkdir src
COPY ./src/requirements.txt .

# install python dependencies
RUN pip3 install --no-cache-dir -r /usr/src/requirements.txt

#copy application
COPY / .

# run application
#CMD ["python3", "/usr/src/index.py"]
#CMD ["python3", "/usr/src/run_me_ingest.py"]
CMD ["tail","-f", "/dev/null"]
