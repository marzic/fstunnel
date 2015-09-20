import os
from threading import Thread, Lock
import socket
import time
import logging
import argparse

import common

logging.basicConfig(format = '%(asctime)s; %(levelname)s; %(name)s; %(thread)d; %(filename)s:%(lineno)d; %(funcName)s; %(message)s', level = logging.INFO)
logger = logging.getLogger(__name__)

FILE_READ_DIR = 'b'
FILE_READ_LOOP_WAIT = 0.05
FILE_READ_MAX_TIME_WITHOUT_FILE = 120

FILE_WRITE_DIR = 'a'
FILE_WRITE_TARGET_SIZE = 1024 * 128

LISTDIR_LOOP_WAIT = 0.05

SOCKET_LOOP_READ_TIMEOUT = 0.01

argument_parser = argparse.ArgumentParser(description = 'TCP tunnel from point a.py to point b.py', epilog = 'glhf')
argument_parser.add_argument('host', help = 'The hostname or IP address to connect to')
argument_parser.add_argument('port', type = int, help = 'The TCP port number to connect to')
args = argument_parser.parse_args()

logger.info("Starting fstunnel point B! (tunneling from dir '%s' to %s:%s, writing responses to dir '%s')", FILE_READ_DIR, args.host, args.port, FILE_WRITE_DIR)

logger.debug("Setting up dirs and cleaning read dir ... [file_read_dir = '%s'; file_write_dir = '%s']", FILE_READ_DIR, FILE_WRITE_DIR)
os.makedirs(FILE_READ_DIR, exist_ok = True)
os.makedirs(FILE_WRITE_DIR, exist_ok = True)
common.clean_dir(FILE_READ_DIR)
logger.debug("Set up dirs and cleaned read dir! [file_read_dir = '%s'; file_write_dir = '%s']", FILE_READ_DIR, FILE_WRITE_DIR)

started_unread_sock_uuids = set()
started_unread_sock_uuids_lock = Lock()

logger.debug('Starting file deleter thread ...')
Thread(target = common.file_deleter, args = (started_unread_sock_uuids, started_unread_sock_uuids_lock), daemon = True).start()
logger.debug('Started file deleter thread!')

while True:
	time.sleep(LISTDIR_LOOP_WAIT)

	try:
		file_read_dir_files = os.listdir(FILE_READ_DIR)
	except:
		logger.exception("Couldn\'t list dir - will sleep for a bit and try again ... [dir_path = '%s']", FILE_READ_DIR)
		continue

	for file_name in file_read_dir_files:
		file_path = os.path.join(FILE_READ_DIR, file_name)
		if not file_name.endswith('.0.dat') or not os.path.isfile(file_path):
			continue

		upstream_socket_uuid = file_name[:32]
		with started_unread_sock_uuids_lock:
			if upstream_socket_uuid in started_unread_sock_uuids:
				continue
			started_unread_sock_uuids.add(upstream_socket_uuid)

		logger.info("New file incoming ... [sock_uuid = '%s'; file_name = '%s'; len(started_unread_sock_uuids) = %s]", upstream_socket_uuid, file_name, len(started_unread_sock_uuids))

		upstream_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		logger.debug("Connecting ... [sock_uuid = '%s']", upstream_socket_uuid)
		upstream_socket.connect((args.host, args.port))
		logger.debug("Connected! [sock_uuid = '%s']", upstream_socket_uuid)

		logger.debug("Starting threads ... [sock_uuid = '%s']", upstream_socket_uuid)
		upstream_socket_dict = {}
		Thread(target = common.files_to_socket, args = (upstream_socket, upstream_socket_dict, upstream_socket_uuid, FILE_READ_DIR, FILE_READ_LOOP_WAIT, FILE_READ_MAX_TIME_WITHOUT_FILE), daemon = True).start()
		Thread(target = common.socket_to_files, args = (upstream_socket, upstream_socket_dict, upstream_socket_uuid, FILE_WRITE_DIR, FILE_WRITE_TARGET_SIZE, SOCKET_LOOP_READ_TIMEOUT), daemon = True).start()
		logger.debug("Started threads! [sock_uuid = '%s']", upstream_socket_uuid)

		break