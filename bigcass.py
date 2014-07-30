#!/usr/bin/env python
#

import os
import sys
import subprocess
import time
import traceback

from ConfigParser import SafeConfigParser
from threading import current_thread

import requests
import futures
import argparse
from prettytable import PrettyTable

import yaml
from yaml.representer import SafeRepresenter

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.compute.base import NodeImage
from libcloud.compute.base import NodeSize


BASE_DIR = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
SUPERNOVA_FILE = os.path.abspath(os.path.expanduser('~/.supernova'))
CLOUD_CONFIG_BASE_FILE = os.path.join(BASE_DIR, 'base.yml')
INJECT_SCRIPTS_DIR = os.path.join(BASE_DIR, 'injected-scripts')
INJECT_UNITS_DIR = os.path.join(BASE_DIR, 'injected-units')

CONCURRENCY = 10

def get_creds(region):
	parser = SafeConfigParser()
	parser.read(SUPERNOVA_FILE)
	user = parser.get(region, 'OS_USERNAME')
	apikey = parser.get(region, 'OS_PASSWORD')

	return user, apikey

def my_driver(conf):
	if conf.driver is None:
		# TODO: other providers
		conf.driver = get_driver(Provider.RACKSPACE)
	return conf.driver

def get_conn(conf):
	my_driver(conf)

	thd = str(current_thread().ident)

	user, apikey = get_creds(conf.region)

	if not conf.conn.has_key(thd):
		conf.conn[thd] = conf.driver(user, apikey, region=conf.region)
	return conf.conn[thd]

def get_nodes(conf, role=None):

	nodes = []

	if role is None or role == "cass":
		cass = [InstanceInfo('cass', conf, "%s-cass-%d" % (conf.prefix, i),
					conf.cass.flavor, conf.cass.image)
							 for i in range(0, conf.cass.count)]
		nodes.extend(cass)

	if role is None or role == "loader":
		loader = [InstanceInfo('loader',conf, "%s-load-%d" % (conf.prefix, i),
					conf.loader.flavor, conf.loader.image)
							 for i in range(0, conf.loader.count)]
		nodes.extend(loader)
	return nodes

def get_node_names(conf, role=None):
	nodes = get_nodes(conf, role)
	names = [n.name for n in nodes]
	return sorted(names)

def get_missing_nodes(conf):
	conn = get_conn(conf)
	expected = dict((n.name, n) for n in get_nodes(conf))
	nodes = dict((n.name, n) for n in conn.list_nodes())
	missing = set(expected.keys()) - set(nodes.keys())
	return [expected[m] for m in missing]

def file_contents(p):
	with open(p) as f:
		return f.read()

def dir_files(d):
	return [f for f in os.listdir(d) if os.path.isfile(os.path.join(d, f))]

def get_units_for_node(conf, instance):
	units = []

	# TODO: make easier to extend
	units.append('coreos-onmetal-env.service')

	if instance.flavor == 'onmetal-io1':
		units.append('apply-lsi-settings.service')
		units.append('setup-lsi-cards-in-raid0.service')
		units.append('media-data.mount')

	# both loader and server want the container created.
	units.append('cassandra-container-creation.service')

	if instance.role == 'cass':
		units.append('cassandra-server.service')
	if instance.role == 'loader':
		units.append('cassandra-stressd.service')

	return units


def get_runcmd_for_node(conf, instance):
	cmds = []

	cmds.append('/opt/bin/debian-time-sync')
	cmds.append('/opt/bin/debian-increase-limits')
	# both loader and server want the cassandra data
	cmds.append('/opt/bin/cassandra-download')
	cmds.append('/opt/bin/oracle-java-install')

	# TODO: make easier to extend
	if instance.flavor == 'onmetal-io1':
		cmds.append('/opt/bin/lsi-cards-apply-settings')
		cmds.append(['/opt/bin/lsi-cards-make-raid0', instance.name])
		cmds.append('/opt/bin/debian-mount-md0')

	cmds.append('/opt/bin/cassandra-docker-import')

	# ran using sudo so new limits.conf applies.
	cmds.append(['sudo', '-i', 'service', 'docker.io', 'restart'])
	if instance.role == 'cass':
		# TODO: sysv init?
		cmds.append('/opt/bin/debian-start-cassandra')
		pass

	if instance.role == 'loader':
		# units.append('cassandra-stressd.service')
		pass

	return cmds



def get_cloud_config(conf, instance):
	cc = yaml.safe_load(file_contents(CLOUD_CONFIG_BASE_FILE))
	if not cc.has_key('write_files'):
		cc['write_files'] = []
	injectfiles = dir_files(INJECT_SCRIPTS_DIR)
	for f in injectfiles:
		fobj = {
			'path': os.path.join('/opt/bin', f),
			'permissions': 755,
			'content': file_contents(os.path.join(INJECT_SCRIPTS_DIR, f))
		}
		cc['write_files'].append(fobj)

	osimg = os_flavor(instance.image)
	if osimg == 'coreos':
		injectunits = get_units_for_node(conf, instance)
		for unit in injectunits:
			uobj = {
				'name': unit,
				'command': 'start',
				'content': file_contents(os.path.join(INJECT_UNITS_DIR, unit))
			}
			cc['coreos']['units'].append(uobj)
		cc['coreos']['etcd']['discovery'] = conf.getDiscoveryUrl()

	if osimg == 'debian':
		cc['apt_upgrade'] = True
		cc['apt_sources'] = [
			{
				'source': 'deb http://ppa.launchpad.net/webupd8team/java/ubuntu precise main',
			 	'key': file_contents(os.path.join(BASE_DIR, 'public-keys', 'webupd8team-java.gpg')),
			 }
		]

		cc['packages'] = [
			'ntpdate',
			'ntp',
			'mdadm',
			'docker.io',
			'sysstat',
			'nload',
		]

		cc['runcmd'] = get_runcmd_for_node(conf, instance)

	# TODO: consider repersentation hacks in http://stackoverflow.com/a/20863889
	ystr = yaml.safe_dump(cc,
			default_flow_style=False)
	ystr = "#cloud-config\n" + ystr
	return ystr

def get_running_lcnodes(conf, role=None):
	conn = get_conn(conf)
	expected = get_node_names(conf, role)
	nodes = conn.list_nodes()
	found = []
	for n in nodes:
		if n.name in expected:
			found.append(n)
	return found

def status(conf):
	expected = get_node_names(conf)
	nodes = get_running_lcnodes(conf)
	pt = PrettyTable(['state', 'id', 'name', 'public_ip', 'private_ip'])
	for n in nodes:
		pt.add_row([n.state, n.id, n.name, n.public_ips, n.private_ips])
	nodenames = [n.name for n in nodes]
	missing = set(expected) - set(nodenames)
	for m in missing:
		pt.add_row(['MISSING', '', m, '', ''])
	print pt

def delete_node(conf, node):
	print 'deleting node: %s' % node.name
	conn = get_conn(conf)
	rv = conn.destroy_node(node)
	return rv, node	

def delete_nodes(conf):
	names = get_node_names(conf)
	todelete = []
	conn = get_conn(conf)
	for n in conn.list_nodes():
		if n.name in names:
			todelete.append(n)

	pt = PrettyTable(['delete-success', 'uuid', 'name'])
	with futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as e:
		returns = []
		for node in todelete:
			returns.append(e.submit(delete_node, conf, node))
		for rv in returns:
			try:
				n = rv.result()
				pt.add_row([n[0], n[1].uuid, n[1].name])
			except Exception as exc:
				traceback.print_exc(file=sys.stdout)
				pt.add_row(['EXCEPTION', '', str(exc)])
	print pt

def create_node(conf, ni):
	print 'booting node: %s flavor=%s' % (ni.name, ni.flavor)
	size = ni.asLibcloudSize()
	image = ni.asLibcloudImage()
	conn = get_conn(conf)
	node = conn.create_node(
			name=ni.name,
			size=size,
			image=image,
			ex_keyname=conf.keyname,
			ex_userdata=get_cloud_config(conf, ni))
	return node

def create_nodes(conf):
	osimg = os_flavor(conf.loader.image)
	if osimg == 'coreos':
		print 'etcd discovery url: %s' % (conf.getDiscoveryUrl())

	toboot = get_missing_nodes(conf)
	pt = PrettyTable(['state', 'id', 'name', 'public_ip', 'private_ip'])
	with futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as e:
		returns = []
		for ni in toboot:
			returns.append(e.submit(create_node, conf, ni))
		for rv in returns:
			try:
				n = rv.result()
				pt.add_row([n.state, n.id, n.name, n.public_ips, n.private_ips])
			except Exception as exc:
				traceback.print_exc(file=sys.stdout)
				pt.add_row(['EXCEPTION', '', str(exc), '', ''])

	print pt

def get_benchcmd(conf, loader, targets, mode):
	host = loader.public_ips[-1]

	cmdline = [
		'ssh',
			'-o', 'StrictHostKeyChecking=no',
			'-o', 'UserKnownHostsFile=/dev/null',
			os_login(conf.loader.image) + '@' + host,
			'sudo'
	]

	# TODO: wish there was a better way.
	osimg = os_flavor(conf.loader.image)

	if osimg == 'coreos':
		cmdline.extend([
			'/usr/bin/systemd-nspawn',
				'-D',
				'/opt/cassandra',
                '--share-system',
                '--capability=all',
                '--bind=/dev:/dev',
                '--bind=/dev/pts:/dev/pts',
                '--bind=/proc:/proc',
             	'/opt/cassandra/tools/bin/cassandra-stress',
			])
	elif osimg == 'docker-bogus':
		cmdline.extend([
			'docker',
				'run',
				'-i',
				'-t',
				'--net=host',
				'--privileged=true',
				'--volume=/media/data/cassandra:/var/lib/cassandra',
				'--volume=/media/data/cassandra-conf:/opt/cassandra/conf',
                'cassandra',
             	'/opt/cassandra/tools/bin/cassandra-stress',
			])
	else:
		cmdline.extend([
         	'/opt/cassandra/opt/cassandra/tools/bin/cassandra-stress',
		])

	if mode == 'keyspace':
		stresscmd = [
# Stress --send-to is BROKEN: https://issues.apache.org/jira/browse/CASSANDRA-5978
#  So, just run it in non-daemon mode for now :(  
#			'--send-to',
#			'127.0.0.1',
			'--nodes',
			','.join(targets),
			'--replication-factor',
			str(conf.bench_replication_factor),
			'--consistency-level',
			str(conf.bench_consistency_level),
			'--num-keys',
			'1'
		]
		cmdline.extend(stresscmd)
	else:
		stresscmd = [
#			'--send-to',
#			'127.0.0.1',
			'--file',
			'/' + loader.name + '.results2',
			'--nodes',
			','.join(targets),
			'--replication-factor',
			str(conf.bench_replication_factor),
			'--consistency-level',
			str(conf.bench_consistency_level),
			'--num-keys',
			str(conf.bench_num_keys),
			'-K',
			str(conf.bench_retries),
			'-t',
			str(conf.bench_threads),
		]
		cmdline.extend(stresscmd)

	return cmdline

def run_cmd(conf, loader, cmd):
	print '%s: %s' % (loader.name, ' '.join(cmd))
	return subprocess.check_output(cmd)

def sshtest(conf):
	nodes = get_running_lcnodes(conf)


	pt = PrettyTable(['name', 'status', 'detail'])
	with futures.ThreadPoolExecutor(max_workers=len(nodes)) as e:
		returns = {}
		for node in nodes:
			cmd = [
				'ssh',
					'-o', 'StrictHostKeyChecking=no',
					'-o', 'UserKnownHostsFile=/dev/null',
					os_login(conf.loader.image) + '@' + node.public_ips[-1],
					'uptime',
			]
			returns[node.name] = e.submit(run_cmd, conf, node, cmd)

		for key in returns.keys():
			try:
				n = returns[key].result()
				pt.add_row([key, 'OK', n.strip()])
			except Exception as exc:
				traceback.print_exc(file=sys.stdout)
				pt.add_row([key, 'EXCEPTION', str(exc)])
	print pt.get_string(sortby="status")

def getresults(conf):
	loaders = get_running_lcnodes(conf, role='loader')
	pt = PrettyTable(['name', 'status', 'detail'])

	with futures.ThreadPoolExecutor(max_workers=len(loaders)) as e:
		returns = {}
		for loader in loaders:
			fname = '/' + loader.name + '.results2'
			cmd = [
				'ssh',
					'-o', 'StrictHostKeyChecking=no',
					'-o', 'UserKnownHostsFile=/dev/null',
					os_login(conf.loader.image) + '@' + loader.public_ips[-1],
					'cat', fname
			]

			returns[loader.name] = e.submit(run_cmd, conf, loader, cmd)

		for key in returns.keys():
			try:
				n = returns[key].result()
				pt.add_row([key, 'OK', ''])
				with open(key + '.results', 'w') as fp:
					fp.write(n)
			except Exception as exc:
				traceback.print_exc(file=sys.stdout)
				pt.add_row([key, 'EXCEPTION', str(exc)])
	print pt

def benchmark(conf):
	loaders = get_running_lcnodes(conf, role='loader')
	cass = get_running_lcnodes(conf, role='cass')
	cassips = [c.private_ips[-1] for c in cass]

	print 'Running with keys=1 to establish keyspace....'
	cmd = get_benchcmd(conf, loaders[0], cassips, 'keyspace')
	run_cmd(conf, loaders[0], cmd)
	print 'Sleeping for 20 seconds to allow keyspace propogation.'
	time.sleep(20)
	print 'Done!'
	print ''

	pt = PrettyTable(['name', 'status', 'detail'])

	with futures.ThreadPoolExecutor(max_workers=len(loaders)) as e:
		returns = {}
		for loader in loaders:
			cmd = get_benchcmd(conf, loader, cassips, 'benchmark')
			returns[loader.name] = e.submit(run_cmd, conf, loader, cmd)

		for key in returns.keys():
			try:
				n = returns[key].result()
				pt.add_row([key, 'OK', ''])
			except Exception as exc:
				traceback.print_exc(file=sys.stdout)
				pt.add_row([key, 'EXCEPTION', str(exc)])
	print pt

class InstanceInfo(object):
	def __init__(self, role, conf, name, flavor, image):
		self.role = role
		self.driver = my_driver(conf)
		self.name = name
		self.flavor = flavor
		self.image = image

	def asLibcloudSize(self):
		return NodeSize(self.flavor, 'dummy size', None, None, None, None,
                        driver=self.driver)

	def asLibcloudImage(self):
		return NodeImage(id=self.image,
						name='dummy image',
						driver=self.driver,
						extra={})

class InstanceConfig(object):
	def __init__(self, count, flavor, image):
		self.count = count
		self.flavor = flavor
		self.image = image

class Config(object):
	def __init__(self, args):
		# TODO: figure out stuff.
		self.image = args.image
		self.cass = InstanceConfig(args.cassandra_count, args.cassandra_flavor, self.image)
		self.loader = InstanceConfig(args.loader_count, args.loader_flavor, self.image)
		self.region = args.region
		self.discovery_url = args.discovery_url
		self.prefix = 'pq'
		self.keyname = 'pquerna'
		self.conn = {}
		self.driver = None
		# TODO add argsparse:
		self.bench_replication_factor = 1
		self.bench_consistency_level = 'quorum'
#		self.bench_consistency_level = 'one'
		self.bench_threads = 500
		self.bench_retries = 100
		self.bench_num_keys = 1500000
#		self.bench_num_keys = 1

	def getDiscoveryUrl(self):
		if len(self.discovery_url) > 0:
			return self.discovery_url

		r = requests.get('https://discovery.etcd.io/new')
		self.discovery_url = r.text.strip()
		return self.discovery_url

def os_flavor(image):
	# TODO: this is horrible
	m = {
		'0372e576-873d-4a21-8466-d60232fa341c': 'coreos', # CoreOS - VM
		'be25b5fd-4ed5-4297-a37a-b886b3546821': 'coreos', # CoreOS - OnMetal
		'64b92981-69c6-4e8a-828b-4a20a8db9adc': 'coreos',
		'bc5afff1-1d0c-4cc5-ba7b-01c0a74c2fbd': 'debian', # Debian - Jessie
	}
	return m[image]

def os_login(image):
	osf = os_flavor(image)
	l = {
		'coreos': 'core',
		'debian': 'root',
	}
	return l[osf]

def main():
	parser = argparse.ArgumentParser(description='Cassandra Cluster Benchmark Manager')
	parser.add_argument('mode', metavar='mode', type=str,
                   help='mode to operate in.', nargs=1,
                   choices=['status', 'create', 'sshtest', 'benchmark', 'results', 'destroy'])

	parser.add_argument('--image', metavar='UUID', type=str,
                   help='base image to use',
                   # CoreOS - VM:
                   #   0372e576-873d-4a21-8466-d60232fa341c
                   # CoreOS - OnMetal:
                   #   64b92981-69c6-4e8a-828b-4a20a8db9adc
                   # Debian jessie: bc5afff1-1d0c-4cc5-ba7b-01c0a74c2fbd
                   default='64b92981-69c6-4e8a-828b-4a20a8db9adc')

	parser.add_argument('--cassandra-count', metavar='N', type=int,
                   help='number of cassandra instances.', default=1)
	# onmetal-io1
	# performance1-1
	parser.add_argument('--cassandra-flavor', metavar='flavor-type', type=str,
                   help='number of cassandra instances.', default='onmetal-io1')

	# onmetal-compute1
	# performance1-1
	parser.add_argument('--loader-count', metavar='N', type=int,
                   help='number of loader instances.', default=1)
	parser.add_argument('--loader-flavor', metavar='flavor-type', type=str,
                   help='flavor type for loader instances.', default='onmetal-compute1')


	parser.add_argument('--region', metavar='region', type=str,
                   help='Region to run in', default='iad')

	parser.add_argument('--discovery-url', metavar='url', type=str,
                   help='etcd discovery url', default='')

	args = parser.parse_args()

	conf = Config(args)
	cmds = {
		'status': status,
		'benchmark': benchmark,
		'results': getresults,
		'sshtest': sshtest,
	 	'create': create_nodes,
	 	'destroy': delete_nodes,
	}

	cmds[args.mode[0]](conf)

if __name__ == '__main__':
	main()
