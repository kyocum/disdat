FROM ubuntu:16.04

RUN apt-get -y update && apt-get install -y --no-install-recommends \
         wget \
         python \
         ca-certificates

RUN wget https://bootstrap.pypa.io/get-pip.py && python get-pip.py
COPY run.py /home/run.py
ENV PIPES_CLASS=df_dup.DFDup

ENTRYPOINT ["/usr/bin/python", "/home/run.py"]