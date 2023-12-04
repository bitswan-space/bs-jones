FROM public.ecr.aws/bitswan/bitswan-pre:sha256_0797b88ecccb8d491e16982e7ab06f97d827b0a482b56261909191f3e29d1e8e

LABEL src=https://gitlab.com/LibertyAces/O2SK/bs-jones

RUN set -ex \
&& apt-get -y update \
&& apt-get -y upgrade

ENV LANG C.UTF-8

RUN apt-get update
RUN apt-get -y install unixodbc-dev
RUN apt-get -y install curl
RUN apt-get -y install wget

# Install dependencies

RUN apt-get install -y --reinstall build-essential
RUN pip3 install pyodbc
RUN apt-get -y install vim
RUN apt-get install -y git

RUN mkdir -p /opt/bs-jones

COPY ./bs_jones.py /opt/bs-jones
COPY ./bs_jones /opt/bs-jones/bs_jones

WORKDIR /opt/bs-jones

CMD ["python3", "bs_jones.py", "-c", "/conf/bs-jones.conf"]
