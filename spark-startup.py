#! /usr/bin/python

# Startup script for HDFS 2.7.1 + Spark 1.5.1

import urllib2
from subprocess import STDOUT, check_call
import os
import subprocess
from pwd import getpwnam
import time


def get_external_ip():
    req = urllib2.Request(
        'http://metadata/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip')
    req.add_header('Metadata-Flavor', 'Google')
    res = urllib2.urlopen(req)
    return res.read()


def get_instance_hostname():
    req = urllib2.Request(
        'http://metadata/computeMetadata/v1/instance/hostname')
    req.add_header('Metadata-Flavor', 'Google')
    res = urllib2.urlopen(req)
    return res.read().split('.')[0]


def get_instance_group_name():
    # instance-group-X
    return 'instance-group-%d' % int(get_instance_hostname()[len('instance-group-')])


def get_hosts():
    lines = subprocess.check_output(['sudo', 'gcloud', 'compute', 'instance-groups', 'list-instances', get_instance_group_name(), '--zone', 'us-central1-f'])
    lines = lines.split('\n')
    lines = lines[1:]
    return sorted(filter(lambda l: len(l) > 0, map(lambda l: l.split(' ')[0], lines)))

username = 'yosub_shin_0'
uid = getpwnam(username)[2]
os.setgid(uid)
os.setuid(uid)

# Let's install binaries first before we get hosts list (May potentially not get every host)
print('Installing binaries...')
check_call(['sudo', 'apt-get', 'install', '-y', 'openjdk-7-jre', 'git', 'emacs', 'byobu', 'apt-transport-https'],
           stdout=open(os.devnull, 'wb'), stderr=STDOUT)

instance_group_hosts = get_hosts()
print('instance group hosts: %s' % ', '.join(instance_group_hosts))
master_hostname = instance_group_hosts[0]
print('master hostname: %s' % master_hostname)
instance_hostname = get_instance_hostname()
print('Current instance hostname: %s' % instance_hostname)
if instance_hostname == master_hostname:
    print('Current host is the master node')

name_node_hostname = master_hostname
home_directory_path = '/home/%s' % username
os.chdir(home_directory_path)

with open('.bashrc', 'a') as bashrc:
    bashrc.write('''
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/jre
''')
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-7-openjdk-amd64/jre'

# Download and install Hadoop 2.7.1
print('Downloading Hadoop...')
check_call(['wget', 'http://mirrors.gigenet.com/apache/hadoop/common/hadoop-2.7.1/hadoop-2.7.1.tar.gz'],
           stdout=open(os.devnull, 'wb'), stderr=STDOUT)
check_call(['tar', '-xzf', 'hadoop-2.7.1.tar.gz'],
           stdout=open(os.devnull, 'wb'), stderr=STDOUT)

hadoop_path = '%s/hadoop-2.7.1' % home_directory_path
hadoop_bashrc_env = '''
export HADOOP_PREFIX="%s"
export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_COMMON_HOME=$HADOOP_PREFIX
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
export HADOOP_HDFS_HOME=$HADOOP_PREFIX
export HADOOP_MAPRED_HOME=$HADOOP_PREFIX
export HADOOP_YARN_HOME=$HADOOP_PREFIX
'''
with open('.bashrc', 'a') as bashrc:
    bashrc.write(hadoop_bashrc_env % hadoop_path)
os.environ['HADOOP_PREFIX'] = hadoop_path
os.environ['HADOOP_HOME'] = hadoop_path
os.environ['HADOOP_COMMON_HOME'] = hadoop_path
os.environ['HADOOP_CONF_DIR'] = '%s/etc/hadoop' % hadoop_path
os.environ['HADOOP_HDFS_HOME'] = hadoop_path
os.environ['HADOOP_MAPRED_HOME'] = hadoop_path
os.environ['HADOOP_YARN_HOME'] = hadoop_path

hadoop_hdfs_site_xml_str ='''<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file://%s/hdfs/datanode</value>
        <description>Comma separated list of paths on the local filesystem of a DataNode where it should store its blocks.</description>
    </property>

    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file://%s/hdfs/namenode</value>
        <description>Path on the local filesystem where the NameNode stores the namespace and transaction logs persistently.</description>
    </property>
</configuration>
'''
with open('%s/etc/hadoop/hdfs-site.xml' % hadoop_path, 'w') as hadoop_hdfs_site_xml:
    hadoop_hdfs_site_xml.write(hadoop_hdfs_site_xml_str % (hadoop_path, hadoop_path))

hadoop_core_site_xml_str ='''<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://%s/</value>
        <description>NameNode URI</description>
    </property>
</configuration>
'''
with open('%s/etc/hadoop/core-site.xml' % hadoop_path, 'w') as hadoop_core_site_xml:
    hadoop_core_site_xml.write(hadoop_core_site_xml_str % name_node_hostname)

# Hardcode JAVA_HOME into hadoop-config.sh (hadoop command doesn't get environments correctly)
hadoop_config_sh = open('%s/libexec/hadoop-config.sh' % hadoop_path, 'r')
hadoop_config_sh_lines = hadoop_config_sh.readlines()
hadoop_config_sh.close()
hadoop_config_sh_lines.insert(165, '''
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/jre
''')
hadoop_config_sh = open('%s/libexec/hadoop-config.sh' % hadoop_path, 'w')
hadoop_config_sh.write("".join(hadoop_config_sh_lines))
hadoop_config_sh.close()

# Write slaves host file
if instance_hostname == master_hostname:
    with open('%s/etc/hadoop/slaves' % hadoop_path, 'w') as hadoop_slaves:
        hadoop_slaves.write('\n'.join(instance_group_hosts))

# Download Spark
print('Downloading Spark...')
check_call(['wget', 'http://apache.mirrors.ionfish.org/spark/spark-1.5.0/spark-1.5.0-bin-hadoop2.6.tgz'],
           stdout=open(os.devnull, 'wb'), stderr=STDOUT)
check_call(['tar', '-xzf', 'spark-1.5.0-bin-hadoop2.6.tgz'],
           stdout=open(os.devnull, 'wb'), stderr=STDOUT)

spark_path = '%s/spark-1.5.0-bin-hadoop2.6' % home_directory_path

check_call(['cp', '%s/conf/spark-env.sh.template' % spark_path, '%s/conf/spark-env.sh' % spark_path],
           stdout=open(os.devnull, 'wb'), stderr=STDOUT)

spark_env_sh_str = '''
export SPARK_DIST_CLASSPATH=$(%s/bin/hadoop classpath)
export SPARK_PUBLIC_DNS=%s
'''
with open('%s/conf/spark-env.sh' % spark_path, 'a') as spark_env_sh:
    spark_env_sh.write(spark_env_sh_str % (hadoop_path, get_external_ip()))

spark_defaults_conf_str = '''
spark.driver.memory                2g
spark.executor.memory              20g
'''
with open('%s/conf/spark-defaults.conf' % spark_path, 'w') as spark_defaults_conf:
    spark_defaults_conf.write(spark_defaults_conf_str)

# Write slaves host file (Only do it at master)
if instance_hostname == master_hostname:
    with open('%s/conf/slaves' % spark_path, 'w') as spark_slaves:
        spark_slaves.write('\n'.join(instance_group_hosts))

id_rsa_str = '''
-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAqSnSqvGPL2arxJKLLw2uU++uDW6xrq7HKrrEuWZM9REUxFRW
w7W+mQ4L+FK9tl8/RDT/0SKGffOinnZu3PtG3i3++D+XbyCq67MdhocGBETWNe4b
McW53uZC+Pk6ahgW65MTAEyRtJKqKrulGDVqPVJdw4JJAgV+LOxesqnhtr8tk83P
g3k5u/M62/D6XPctKE3MfHb03wtuF5242/1qnvMoQYMrGN7jRt/5Eo98iceNwZvf
XOoPYCwtrwb8neGw0BPfvW7YjrvslX6IJtmyRXGmsGBokA9Bqd6ZDZkq/bYZiHfu
/GqPMLptoxNBtV72dPKAEB1lbP7PYez8C1XGgwIDAQABAoIBAGSA9aocdH6sGFdk
3Y6qKS2zVAyk/KoVKz2m02R3dDeR22290gLbAw96OgBiYFZvBm6msmp1gcRpMO/G
250tKXCtkTO6zGT42rPIqj0YEaoNn9tQyRVsLT9SPO4hXORVxaBWtE5UL6lCDhnv
fGoCqkkem5ih2nB6BPn5wVWS+wiQW59ES1AoTfa8ULBVCeNAZhcPDSVaiwPS9Rfv
4ecrSUaZOOAK/piuH+aSi1DhADAXYXrBGCUff9loniewA1i4w3w8l7jotAnOETA8
E6XUQCZg6raT5HIOZGaCO5Y/QuDMzgDd8IEy88lxjlEjdSd4kEp28iozyyScDA7d
bb9WZDECgYEA2gAzcMDOpwJ51JUCiyfy2GjauC90AIGNLukjefGtx5IqNH8UU5uA
SLZ28MfWAeDXUjEvNkwmqS3KQRp6E2WTO9sUA7WbFxLc294P+IMDD6pUY6nxpI37
a2liaxGVVUSCoMEaBQpTkXNCC1TkgIQ8dsS59RaeUVJrcAfSznC3jTkCgYEAxqZc
en1YTYZ45aUaWiPIXgjw2+ZyqcrTDFH30USt7b1Tf4r+6oGGz5q5HP4vo76oVZYF
uHtV1pRDkJ/Kx2hraRwADOkMT9l+paN7gDxs3iR5FXPVA/5ggiKyie9zoXDa2jY1
4m4KQYrqOhb+8qrNR159KmrP4S4P2OmgEX+ibZsCgYBGpvoE+PgAuJSziPeiEfhq
mtEIEJkP8OzI31ZYFzOzEnQLP4Re9G7HIhu6PYnmYfBm+vnKJzQAcI60OtiFoM3v
ADmkWh9BgyOBPp7+c7dyREnFYzallj59uVHkUXaMg/+yCeNc7tPWt/wXoBPOcw0F
kQyTmhkFUijvzhlMPsu+QQKBgB3upVKjnnGYCJF5zj201JUuvbQ0xiRFdoWNuEyl
D5waAgHe3MhTGhAgHTJ8Lot6x/yVbWk91FJP5tpc6X4ggsbEvFE1sHA7snSc7JgH
AtR6JHCSEo/WfY4+Ui6skPzLd36X2oiy0gLMPrzgCCxihinx1+RTUd15RlQF5+Ob
GstvAoGAOsqBVfhcBR560uOJnIuSEkVypV5Fe/JVvkytEKjQpGBIwg0XxP+aS5xH
BncI29j3RkNuHkIsc+dzeMspi/niDZwLL9NwN8BGKTm2MQ64RPsOX7EEf4UHcIFS
tEB6abs71lFks4hTDUKgccp4NZMyWtWM6Wfd5kSfLA/4/NIQjAY=
-----END RSA PRIVATE KEY-----
'''
with open('.ssh/id_rsa', 'w') as id_rsa:
    id_rsa.write(id_rsa_str)
check_call(['chmod', '0600', '.ssh/id_rsa'],
           stdout=open(os.devnull, 'wb'), stderr=STDOUT)

with open('.ssh/known_hosts', 'w') as known_hosts:
    known_hosts.write('\n'.join(instance_group_hosts))

with open('.ssh/config', 'w') as config:
    config.write('''Host *
    StrictHostKeyChecking no
    UserKnownHostsFile=/dev/null
''')

# Public key
# ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCpKdKq8Y8vZqvEkosvDa5T764NbrGurscqusS5Zkz1ERTEVFbDtb6ZDgv4Ur22Xz9ENP/RIoZ986Kedm7c+0beLf74P5dvIKrrsx2GhwYERNY17hsxxbne5kL4+TpqGBbrkxMATJG0kqoqu6UYNWo9Ul3DgkkCBX4s7F6yqeG2vy2Tzc+DeTm78zrb8Ppc9y0oTcx8dvTfC24Xnbjb/Wqe8yhBgysY3uNG3/kSj3yJx43Bm99c6g9gLC2vBvyd4bDQE9+9btiOu+yVfogm2bJFcaawYGiQD0Gp3pkNmSr9thmId+78ao8wum2jE0G1XvZ08oAQHWVs/s9h7PwLVcaD yosub_shin_0@gmail.com

# Format the namenode directory and write slaves host file (Only do it at master)
if instance_hostname == master_hostname:
    print('Formatting HDFS namenode...')
    check_call(['%s/bin/hdfs' % hadoop_path, 'namenode', '-format'],
               stdout=open(os.devnull, 'wb'), stderr=STDOUT)

# Start HDFS (Only do it at master)
if instance_hostname == master_hostname:
    print('Sleeping for 10 seconds before starting HDFS...')
    time.sleep(10)
    print('Starting HDFS...')
    check_call(['%s/sbin/start-dfs.sh' % hadoop_path],
               stdout=open(os.devnull, 'wb'), stderr=STDOUT)

# Start Spark Standalone cluster
if instance_hostname == master_hostname:
    print('Sleeping for 10 seconds for HDFS to be ready...')
    time.sleep(10)
    print('Starting Spark Standalone cluster...')
    check_call(['%s/sbin/start-all.sh' % spark_path],
               stdout=open(os.devnull, 'wb'), stderr=STDOUT)

