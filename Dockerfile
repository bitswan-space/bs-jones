FROM teskalabs/bspump:nightly
MAINTAINER TeskaLabs Ltd (support@teskalabs.com)

# http://bugs.python.org/issue19846
# > At the moment, setting "LANG=C" on a Linux system *fundamentally breaks Python 3*, and that's not OK.
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

RUN set -ex \
	mkdir -p /opt/bs-jones

COPY ./script.py /opt/bs-jones/script.py

COPY ./site/sybase /opt/sybase

WORKDIR /opt/bs-jones

CMD ["python3", "script.py"]
