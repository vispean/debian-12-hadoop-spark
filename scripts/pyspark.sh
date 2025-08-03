#!/bin/bash
# https://docs.vagrantup.com/v2/provisioning/shell.html

#########
    #
    #  Local Python
    #
    #  shell script for provisioning of a debian 12 machine with python and pyspark.
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

function setUpPython {
    # install python
    apt-get install -y python3
    apt-get install -y python3-venv
    apt-get install -y pip

    # create python virtual environment in the vagrant home directory
    if [ ! -d "/home/vagrant/python" ]; then
        mkdir /home/vagrant/python
    fi
    python3 -m venv /home/vagrant/python
}

function setUpPyspark {
    # install pyspark
    /home/vagrant/python/bin/pip3 install pyspark

    # copy python example script into home directory
    cp /vagrant/auxiliary_files/python/pyspark_SQL_example.py /home/vagrant/pyspark_SQL_example.py
    cp /vagrant/auxiliary_files/python/pyspark_dataframe_api_example.py /home/vagrant/pyspark_dataframe_api_example.py

    # echo command to run python a script with 
    echo "to run a python script with set up virtual environment:"
    echo "$ /home/vagrant/python/bin/python3 /home/vagrant/pyspark_SQL_example.py"
}

echo "################"
echo "# setup python #"
echo "################"
setUpPython

echo "#################"
echo "# setup pyspark #"
echo "#################"
setUpPyspark
