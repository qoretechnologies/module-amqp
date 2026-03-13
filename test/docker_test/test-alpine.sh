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

echo "export QORE_UID=1000" >> ${ENV_FILE}
echo "export QORE_GID=1000" >> ${ENV_FILE}

. ${ENV_FILE}

export MAKE_JOBS=4

# install SASL modules required for proton PLAIN authentication
apk add --no-cache cyrus-sasl cyrus-sasl-plain > /dev/null 2>&1

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
# Start broker directly in background (artemis-service uses "ps -p" which
# is not supported by BusyBox ps on Alpine)
/tmp/amqp-broker/bin/artemis run > /tmp/amqp-broker/log/artemis.log 2>&1 &
BROKER_PID=$!
export AMQP_TEST_URL="amqp://guest:guest@localhost:5672"

# wait for broker to be ready
for i in $(seq 1 30); do
    if /tmp/amqp-broker/bin/artemis check node --up 2>/dev/null; then
        echo "Broker is ready"
        break
    fi
    if ! kill -0 $BROKER_PID 2>/dev/null; then
        echo "ERROR: Broker process died"
        cat /tmp/amqp-broker/log/artemis.log
        exit 1
    fi
    sleep 1
done

# add Qore user and group
if ! grep -q "^qore:x:${QORE_GID}" /etc/group; then
    addgroup -g ${QORE_GID} qore
fi
if ! grep -q "^qore:x:${QORE_UID}" /etc/passwd; then
    adduser -u ${QORE_UID} -D -G qore -h /home/qore -s /bin/bash qore
fi

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
