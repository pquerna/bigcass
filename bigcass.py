#!/usr/bin/env python
#

import os
import sys
import subprocess
import time
import traceback

from ConfigParser import SafeConfigParser
from threading import current_thread

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

	# TODO: stress container service
	return units


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

	injectunits = get_units_for_node(conf, instance)
	for unit in injectunits:
		uobj = {
			'name': unit,
			'command': 'start',
			'content': file_contents(os.path.join(INJECT_UNITS_DIR, unit))
		}
		cc['coreos']['units'].append(uobj)

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
	pt = PrettyTable(['state', 'uuid', 'name', 'public_ip', 'private_ip'])
	for n in nodes:
		pt.add_row([n.state, n.uuid, n.name, n.public_ips, n.private_ips])
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
	toboot = get_missing_nodes(conf)
	pt = PrettyTable(['state', 'uuid', 'name', 'public_ip', 'private_ip'])
	with futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as e:
		returns = []
		for ni in toboot:
			returns.append(e.submit(create_node, conf, ni))
		for rv in returns:
			try:
				n = rv.result()
				pt.add_row([n.state, n.uuid, n.name, n.public_ips, n.private_ips])
			except Exception as exc:
				traceback.print_exc(file=sys.stdout)
				pt.add_row(['EXCEPTION', '', str(exc), '', ''])

	print pt

def get_benchcmd(conf, loader, targets, mode):
	host = loader.public_ips[0]
	cmdline = [
		'ssh',
			'-o', 'StrictHostKeyChecking=no',
			'-o', 'UserKnownHostsFile=/dev/null',
			'core@' + host,
			'sudo',
			'/usr/bin/systemd-nspawn',
				'-D',
				'/opt/cassandra',
                '--share-system',
                '--capability=all',
                '--bind=/dev:/dev',
                '--bind=/dev/pts:/dev/pts',
                '--bind=/proc:/proc',
             	'/opt/cassandra/tools/bin/cassandra-stress',
	]

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
			loader.name + '.results',
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

def benchmark(conf):
	loaders = get_running_lcnodes(conf, role='loader')
	cass = get_running_lcnodes(conf, role='cass')
	cassips = [c.private_ips[0] for c in cass]

	print 'Running with keys=1 to establish keyspace....'
	cmd = get_benchcmd(conf, loaders[0], cassips, 'keyspace')
	run_cmd(conf, loaders[0], cmd)
	print 'Sleeping for 5 seconds, just because.'
	time.sleep(5)
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
		self.prefix = 'pq'
		self.keyname = 'pquerna'
		self.conn = {}
		self.driver = None
		# TODO add argsparse:
		self.bench_replication_factor = 3
		self.bench_consistency_level = 'quorum'
		self.bench_threads = 600
		self.bench_retries = 100
		self.bench_num_keys = 30000000
#		self.bench_num_keys = 1

def main():
	parser = argparse.ArgumentParser(description='Cassandra Cluster Benchmark Manager')
	parser.add_argument('mode', metavar='mode', type=str,
                   help='mode to operate in.', nargs=1,
                   choices=['status', 'create', 'bootstrap', 'benchmark', 'destroy'])

	parser.add_argument('--image', metavar='UUID', type=str,
                   help='base image to use',
                   # CoreOS - VM:
                   #   0372e576-873d-4a21-8466-d60232fa341c
                   # New CoreOS - OnMetal:
                   #   53047266-698a-4a34-8076-bfc9915593d2
                   default='53047266-698a-4a34-8076-bfc9915593d2')

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

	args = parser.parse_args()

	conf = Config(args)
	cmds = {
		'status': status,
		'benchmark': benchmark,
	 	'create': create_nodes,
	 	'destroy': delete_nodes,
	}

	cmds[args.mode[0]](conf)

if __name__ == '__main__':
	main()