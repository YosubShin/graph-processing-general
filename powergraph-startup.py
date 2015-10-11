#! /usr/bin/python

# Startup script for PowerGraph 2.2

import urllib2
from subprocess import STDOUT, check_call
import os
import subprocess
from pwd import getpwnam

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
check_call(['sudo', 'apt-get', 'install', '-y', 'git', 'emacs', 'byobu', 'apt-transport-https', 'gcc', 'g++', 'cmake', 'build-essential', 'zlib1g', 'zlib1g-dev', 'libgomp1', 'openmpi-bin', 'openmpi-doc', 'libopenmpi-dev', 'default-jdk'],
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

power_graph_path = '%s/PowerGraph' % home_directory_path

# Download PowerGraph
print('Downloading PowerGraph...')
check_call(['git', 'clone', 'https://github.com/YosubShin/PowerGraph.git'],
           stdout=open(os.devnull, 'wb'), stderr=STDOUT)

# Write slaves host file (Only do it at master)
if instance_hostname == master_hostname:
    # with open('%s/release/toolkits/graph_analytics/hosts' % power_graph_path, 'w') as hosts_file:
    with open('%s/machines' % home_directory_path, 'w') as hosts_file:
        hosts_file.write('\n'.join(instance_group_hosts))

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

rsync_sh_str = '''
cd ~/PowerGraph/release/toolkits
~/PowerGraph/scripts/mpirsync
cd ~/PowerGraph/deps/local
~/PowerGraph/scripts/mpirsync
cd ~/graphs
~/PowerGraph/scripts/mpirsync
'''
with open('%s/rsync.sh' % home_directory_path, 'w') as rsync_sh:
    rsync_sh.write(rsync_sh_str)
check_call(['chmod', '0700', '%s/rsync.sh' % home_directory_path],
           stdout=open(os.devnull, 'wb'), stderr=STDOUT)

experiment_py_str = '''#! /usr/bin/python

from subprocess import STDOUT, check_call
import os
import subprocess

num_hosts = 3
home_path = '/home/yosub_shin_0'
power_graph_home = '%s/PowerGraph' % home_path
graph_analytics_bin_path = '%s/release/toolkits/graph_analytics' % power_graph_home
num_iterations = [1, 5, 10]
algorithms = ['pagerank']

partitioning_strategies = ['random', 'oblivious', 'hdrf']
graph_path = '%s/graphs/livejournal/' % home_path

hostfile_path = '%s/machines' % home_path

for run in range(1):  # Do experiment X many times
    for num_iteration in num_iterations:
        for algorithm in algorithms:
            algorithm_path = '%s/%s' % (graph_analytics_bin_path, algorithm)
            for partitioning_strategy in partitioning_strategies:
                print('algorithm: %s, partitioning_strategy: %s, num_iteration: %d' % (algorithm, partitioning_strategy, num_iteration))
                check_call(['mpiexec', '-n', str(num_hosts), '-hostfile', hostfile_path, algorithm_path, '--graph=%s' % graph_path, '--format=snap', '--iterations=%d' % num_iteration, '--graph_opts=ingress=%s' % partitioning_strategy],
                           stderr=STDOUT)

'''
with open('%s/experiment.py' % home_directory_path, 'w') as experiment_py:
    experiment_py.write(experiment_py_str)
check_call(['chmod', '0700', '%s/experiment.py' % home_directory_path],
           stdout=open(os.devnull, 'wb'), stderr=STDOUT)
