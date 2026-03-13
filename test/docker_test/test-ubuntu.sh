#!/bin/bash

set -e
set -x

ENV_FILE=/tmp/env.sh

. ${ENV_FILE}

# setup MODULE_SRC_DIR env var
cwd=`pwd`
if [ -z "${MODULE_SRC_DIR}" ]; then
    if [ -e "$cwd/src/amqp-module.cpp" ]; then
        MODULE_SRC_DIR=$cwd
    else
        MODULE_SRC_DIR=$WORKDIR/module-amqp
    fi
fi
echo "export MODULE_SRC_DIR=${MODULE_SRC_DIR}" >> ${ENV_FILE}

echo "export QORE_UID=999" >> ${ENV_FILE}
echo "export QORE_GID=999" >> ${ENV_FILE}

. ${ENV_FILE}

export MAKE_JOBS=4

# install SASL modules required for proton PLAIN authentication
apt-get update -qq && apt-get install -y -qq libsasl2-modules > /dev/null 2>&1

# build module and install
echo && echo "-- building module --"
mkdir -p ${MODULE_SRC_DIR}/build
cd ${MODULE_SRC_DIR}/build
cmake .. -DCMAKE_BUILD_TYPE=debug -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
make -j${MAKE_JOBS}
make install

# create and start Artemis broker instance (Artemis pre-installed in deps image)
echo && echo "-- starting Artemis broker --"
${ARTEMIS_HOME}/bin/artemis create /tmp/amqp-broker \
    --user guest --password guest --allow-anonymous --silent
/tmp/amqp-broker/bin/artemis-service start
export AMQP_TEST_URL="amqp://guest:guest@localhost:5672"

# wait for broker to be ready
for i in $(seq 1 30); do
    if /tmp/amqp-broker/bin/artemis check node --up 2>/dev/null; then
        echo "Broker is ready"
        break
    fi
    sleep 1
done

# add Qore user and group
groupadd -o -g ${QORE_GID} qore
useradd -o -m -d /home/qore -u ${QORE_UID} -g ${QORE_GID} qore

# own everything by the qore user
chown -R qore:qore ${MODULE_SRC_DIR}

# run the tests
export QORE_MODULE_DIR=${MODULE_SRC_DIR}/qlib:${QORE_MODULE_DIR}
cd ${MODULE_SRC_DIR}
FAILED=0
for test in test/*.qtest; do
    if ! gosu qore:qore env AMQP_TEST_URL="${AMQP_TEST_URL}" qore --enable-debug $test -vv; then
        FAILED=1
    fi
done
exit $FAILED
