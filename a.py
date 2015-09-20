from threading import Thread
import socket
import logging
import argparse
import os

import common

logging.basicConfig(format = '%(asctime)s; %(levelname)s; %(name)s; %(thread)d; %(filename)s:%(lineno)d; %(funcName)s; %(message)s', level = logging.INFO)
logger = logging.getLogger(__name__)

MAX_CONNECTS_QUEUED = 20

FILE_READ_DIR = 'a'
FILE_READ_LOOP_WAIT = 0.05
FILE_READ_MAX_TIME_WITHOUT_FILE = 120

FILE_WRITE_DIR = 'b'
FILE_WRITE_TARGET_SIZE = 1024 * 128

SOCKET_LOOP_READ_TIMEOUT = 0.01

argument_parser = argparse.ArgumentParser(description = 'TCP tunnel from point a.py to point b.py', epilog = 'glhf')
argument_parser.add_argument('host', help = 'The local hostname or IP address to listen on (0.0.0.0 to listen on all interfaces)')
argument_parser.add_argument('port', type = int, help = 'The local TCP port number to listen on')
args = argument_parser.parse_args()

logger.info("Starting fstunnel point A! (tunneling from %s:%s to dir '%s', reading responses from dir '%s')", args.host, args.port, FILE_WRITE_DIR, FILE_READ_DIR)

logger.debug("Setting up dirs and cleaning read dir ... [file_read_dir = '%s'; file_write_dir = '%s']", FILE_READ_DIR, FILE_WRITE_DIR)
os.makedirs(FILE_READ_DIR, exist_ok = True)
os.makedirs(FILE_WRITE_DIR, exist_ok = True)
common.clean_dir(FILE_READ_DIR)
logger.debug("Set up dirs and cleaned read dir! [file_read_dir = '%s'; file_write_dir = '%s']", FILE_READ_DIR, FILE_WRITE_DIR)

logger.debug('Starting file deleter thread ...')
Thread(target = common.file_deleter, daemon = True).start()
logger.debug('Started file deleter thread!')

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((args.host, args.port))
server_socket.listen(MAX_CONNECTS_QUEUED)
while True:
	logger.info('Accepting ...')
	client_socket, address = server_socket.accept()
	client_socket_uuid = common.get_uuid_base64()
	logger.info("Accepted! [sock_uuid = '%s']", client_socket_uuid)

	logger.debug("Starting threads ... [sock_uuid = '%s']", client_socket_uuid)
	client_socket_dict = {}
	Thread(target = common.socket_to_files, args = (client_socket, client_socket_dict, client_socket_uuid, FILE_WRITE_DIR, FILE_WRITE_TARGET_SIZE, SOCKET_LOOP_READ_TIMEOUT), daemon = True).start()
	Thread(target = common.files_to_socket, args = (client_socket, client_socket_dict, client_socket_uuid, FILE_READ_DIR, FILE_READ_LOOP_WAIT, FILE_READ_MAX_TIME_WITHOUT_FILE), daemon = True).start()
	logger.debug("Started threads! [sock_uuid = '%s']", client_socket_uuid)