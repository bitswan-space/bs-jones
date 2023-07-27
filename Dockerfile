FROM public.ecr.aws/h9w9d7v3/bspump:2023-39-git-8444683

LABEL src=https://gitlab.com/LibertyAces/O2SK/bs-telco-wifi-scan

RUN set -ex \
&& apt-get -y update \
&& apt-get -y upgrade

ENV LANG C.UTF-8

RUN apt-get update
RUN apt-get -y install unixodbc-dev
RUN apt-get -y install curl
RUN apt-get -y install wget

# Install fastkafka

RUN apt-get update
RUN apt-get -y install gcc
RUN pip3 install --no-cache-dir cython
RUN apt-get -y install git
RUN apt-get -y install curl make gcc g++ libc-dev libz-dev libzstd-dev
RUN curl -O -L https://github.com/edenhill/librdkafka/archive/v1.5.2.tar.gz --silent --fail \
	&& tar xzf v1.5.2.tar.gz \
	&& cd librdkafka-1.5.2 \
	&& ./configure \
	&& cd src \
	&& make \
	&& make install
RUN ldconfig
RUN pip3 install --no-cache-dir git+https://lmiodeploy:bR88sBTwX6ys5b2d47cG@gitlab.com/TeskaLabs/fastkafka@f5dc0a932928f296b1a585a4b9cf2ac23101daf0
RUN pip3 install xxhash

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
