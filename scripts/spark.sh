#!/bin/bash
# https://docs.vagrantup.com/v2/provisioning/shell.html

#########
    #
    #  Spark
    #
    #  shell script for provisioning of a debian 12 machine with spark based on hadoop.
    #
    #  @package     Debian-12-Bookworm-CH
    #  @subpackage  Hadoop-Spark
    #  @author      Christian Locher <locher@faithpro.ch>
    #  @copyright   2025 Faithful programming
    #  @license     http://www.gnu.org/licenses/gpl-3.0.en.html GNU/GPLv3
    #  @version     alpha - 2025-08-02
    #  @since       File available since release alpha
    #
    #########

function updateDebian {
    apt-get update
    apt-get full-upgrade -y
}

function setUpSpark {
    # install java
    apt-get install -y default-jdk

    # install spark
    apt-get install -y wget
    wget https://dlcdn.apache.org/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz
    apt-get purge -y wget
    tar xvf spark-4.0.0-bin-hadoop3.tgz
    mv spark-4.0.0-bin-hadoop3 /opt/spark
    rm spark-4.0.0-bin-hadoop3.tgz
    echo "SPARK_HOME=/opt/spark" >> /etc/environment
    echo "PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> /etc/environment
    source /etc/environment
}

echo "#################"
echo "# update debian #"
echo "#################"
updateDebian

echo "################"
echo "# setup spark #"
echo "################"
setUpSpark