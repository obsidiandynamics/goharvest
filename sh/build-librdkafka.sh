#!/bin/sh

cd $(dirname $0)

set -e

if [ -d librdkafka ]; then
  cd librdkafka
  git pull
  cd ..
else
  git clone https://github.com/edenhill/librdkafka.git
fi

cd librdkafka
./configure --prefix /usr
make
sudo make install
rm -rf librdkafka
