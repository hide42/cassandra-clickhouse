#!/bin/sh

set -e

jar_file="cassandra-trigger*.jar"
settings_file="KafkaTrigger.yml"
mvn_build_dir="target/"
cassandra_dir=${1%/}
if [ ! $1 ]; then
	echo "First argument may be a Cassandra 2.1+ root directory."
        echo "Try use default dir /etc/cassandra"
        cassandra_dir="/etc/cassandra"
fi


if [ ! -d ${cassandra_dir} ]; then
	echo "Directory does not exist - ${cassandra_dir}"
	exit 1
fi

cassandra_triggers_dir=${cassandra_dir}/conf/triggers
if [ ! -d ${cassandra_triggers_dir} ]; then
	echo "Triggers directory does not exist ($cassandra_triggers_dir)."
	echo "Are you sure this is a valid Cassandra 2.1+ installation?"
	exit 1
fi

echo "Building jar with mvn..."
mvn clean install

echo "Uninstalling old jar versions..."
rm -f ${cassandra_triggers_dir}/${jar_file}

echo "Copying new jar into ${cassandra_triggers_dir}..."
cp ${mvn_build_dir}/${jar_file}  ${cassandra_triggers_dir}

if [ ! -f ${cassandra_triggers_dir}/${settings_file} ]; then
    echo "Copying settings file ${settings_file} to ${cassandra_triggers_dir}..."
    cp conf/${settings_file} ${cassandra_triggers_dir}
fi

echo "The trigger was successfully installed."

user=`whoami`
cassandra_pid=`pgrep -u ${user} -f cassandra || true`

if [ ! -z "${cassandra_pid}" ]; then
    echo "Cassandra is running for current user with PID ${cassandra_pid}. Atempting to reload triggers..."
    if nodetool reloadtriggers; then
        echo "Trigger loaded successfuly. You can already use it on the CQL sheel."
    else
        echo "Something went wrong. Could not reload triggers. Try restarting Cassandra manually."
        exit 1
    fi
fi

exit 0
