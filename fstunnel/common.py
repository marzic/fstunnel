import struct
import base64
import os
import time
import logging
from threading import Lock
import select

logger = logging.getLogger(__name__)

FILE_ACCESS_ERROR_BACKOFF = 0.5

def socket_to_files(sock, sock_dict, sock_uuid, write_dir, target_file_size, select_timeout):
	logger.debug("Entering socket_to_files() ... [sock_uuid = '%s'; write_dir = '%s'; target_file_size = %s; select_timeout = %s]", sock_uuid, write_dir, target_file_size, select_timeout)

	i = 0
	end_read = False
	try:
		while not end_read:
			logger.debug("Starting sock.recv() loop ... [sock_uuid = '%s']", sock_uuid)

			chunks = []
			len_chunk_bytes = 0
			while True:
				logger.debug("Entering select() ... [sock_uuid = '%s'; select_timeout = %s]", sock_uuid, select_timeout)
				try:
					ready_socks, _, _ = select.select([sock], [], [], select_timeout)
				except:
					if sock_dict.get('expected_close', False):
						logger.debug("select() threw an expected exception because we closed the socket from the other thread - it\'s fine, most likely we just received an EOF file - breaking loop, dumping chunks, and ending read ... [sock_uuid = '%s'; select_timeout = %s]", sock_uuid, select_timeout)
					else:
						logger.exception("select() threw an exception - breaking loop, dumping chunks, and ending read ... [sock_uuid = '%s'; select_timeout = %s]", sock_uuid, select_timeout)
					end_read = True
					break
				logger.debug("Returned from select()! [sock_uuid = '%s'; select_timeout = %s]", sock_uuid, select_timeout)

				if len(ready_socks) == 0:
					if len_chunk_bytes > 0:
						logger.debug("select() timed out and there are chunks to dump - breaking loop, dumping chunks, and continuing read ... [sock_uuid = '%s'; select_timeout = %s]", sock_uuid, select_timeout)
						break
					else:
						logger.debug("select() timed out and there is nothing to dump - letting loop fall into sock.recv() ... [sock_uuid = '%s'; select_timeout = %s]", sock_uuid, select_timeout)

				logger.debug("Entering sock.recv() ... [sock_uuid = '%s']", sock_uuid)
				try:
					chunk = sock.recv(8192)
				except:
					if sock_dict.get('expected_close', False):
						logger.debug("sock.recv() threw an expected exception because we closed the socket from the other thread - it\'s fine, most likely we just received an EOF file - breaking loop, dumping chunks, and ending read ... [sock_uuid = '%s']", sock_uuid)
					else:
						logger.exception("sock.recv() threw an exception - breaking loop, dumping chunks, and ending read ... [sock_uuid = '%s']", sock_uuid)
					end_read = True
					break
				logger.debug("Returned from sock.recv()! [sock_uuid = '%s']", sock_uuid)

				if len(chunk) == 0:
					logger.debug("Got EOF from sock.recv - breaking loop, dumping chunks, and ending read ... [sock_uuid = '%s']", sock_uuid)
					end_read = True
					break

				chunks.append(chunk)
				len_chunk_bytes += len(chunk)

				if len_chunk_bytes >= target_file_size:
					logger.debug("Chunk bytes reached target file size - breaking loop, dumping chunks, and continuing read ... [sock_uuid = '%s']", sock_uuid)
					break

			if len_chunk_bytes > 0:
				file_path = os.path.join(write_dir, '{}.{}.dat'.format(sock_uuid, i))
				logger.debug("Dumping chunks ... [sock_uuid = '%s', file_path = '%s']", sock_uuid, file_path)
				put_file(file_path, chunks, sock_uuid)
				logger.debug("Dumped chunks! [sock_uuid = '%s', file_path = '%s']", sock_uuid, file_path)
				i += 1
	finally:
		file_path = os.path.join(write_dir, '{}.{}.dat'.format(sock_uuid, i))
		logger.debug("Dumping EOF ... [sock_uuid = '%s', file_path = '%s']", sock_uuid, file_path)
		put_file(file_path, (), sock_uuid)
		logger.debug("Dumped EOF! [sock_uuid = '%s', file_path = '%s']", sock_uuid, file_path)

	logger.debug("Returning from socket_to_files() ... [sock_uuid = '%s'; write_dir = '%s'; target_file_size = %s; select_timeout = %s]", sock_uuid, write_dir, target_file_size, select_timeout)

def files_to_socket(sock, sock_dict, sock_uuid, read_dir, loop_wait, max_time_without_file):
	logger.debug("Entering files_to_socket ... [sock_uuid = '%s'; read_dir = '%s'; loop_wait = %s; max_time_without_file = %s]", sock_uuid, read_dir, loop_wait, max_time_without_file)

	i = 0
	time_without_file = 0
	try:
		while True:
			time.sleep(loop_wait)
			file_path = os.path.join(read_dir, '{}.{}.dat'.format(sock_uuid, i))
			if os.path.isfile(file_path):
				i += 1
				time_without_file = 0

				logger.debug("Reading file ... [sock_uuid = '%s'; file_path = '%s']", sock_uuid, file_path)
				file_bytes = get_file(file_path, sock_uuid)
				delete_file(file_path)
				logger.debug("Read file and queued it for deletion [sock_uuid = '%s'; file_path = '%s']", sock_uuid, file_path)

				if len(file_bytes) == 0:
					logger.debug("Got EOF from file - breaking file read loop and closing socket ... [sock_uuid = '%s'; file_path = '%s']", sock_uuid, file_path)
					break

				logger.debug("Sending bytes read from file with sock.sendall() ... [sock_uuid = '%s'; file_path = '%s']", sock_uuid, file_path)
				sock.sendall(file_bytes)
				logger.debug("Sent bytes read from file with sock.sendall()! [sock_uuid = '%s'; file_path = '%s']", sock_uuid, file_path)
			else:
				time_without_file += loop_wait
				logger.debug("Waiting on file ... [sock_uuid = '%s'; file_path = '%s'; time_without_file = %s; max_time_without_file = %s]", sock_uuid, file_path, time_without_file, max_time_without_file)
				if time_without_file > max_time_without_file:
					logger.warning("Waiting on file for too long - breaking file read loop and closing socket ... [sock_uuid = '%s'; file_path = '%s'; time_without_file = %s; max_time_without_file = %s]", sock_uuid, file_path, time_without_file, max_time_without_file)
					break
	finally:
		logger.debug("Closing socket ... [sock_uuid = '%s']", sock_uuid)
		sock_dict['expected_close'] = True
		try:
			sock.close()
		except:
			logger.exception("sock.close() threw an exception - pressing on ... [sock_uuid = '%s']", sock_uuid)
		else:
			logger.debug("Closed socket! [sock_uuid = '%s']", sock_uuid)

		logger.debug("Queueing socket files for deletion ... [sock_uuid = '%s'; read_dir = '%s']", sock_uuid, read_dir)
		delete_socket_files(sock_uuid, read_dir)
		logger.debug("Queued socket files for deletion! [sock_uuid = '%s'; read_dir = '%s']", sock_uuid, read_dir)

	logger.debug("Returning from files_to_socket() ... [sock_uuid = '%s'; read_dir = '%s'; loop_wait = %s; max_time_without_file = %s]", sock_uuid, read_dir, loop_wait, max_time_without_file)

def get_file(file_path, sock_uuid):
	while True:
		logger.debug("Reading .dat file ... [sock_uuid = '%s'; file_path = '%s']", sock_uuid, file_path)
		try:
			with open(file_path, 'rb') as f:
				file_bytes = f.read()
		except:
			logger.exception("Couldn\'t read .dat file - will sleep for a bit and try again ... [sock_uuid = '%s'; file_path = '%s']", sock_uuid, file_path)
			time.sleep(FILE_ACCESS_ERROR_BACKOFF)
			continue
		logger.debug("Read .dat file! [sock_uuid = '%s'; file_path = '%s']", sock_uuid, file_path)
		return file_bytes

def put_file(file_path, chunks, sock_uuid):
	tmp_file_path = file_path + '.tmp'
	while True:
		logger.debug("Writing .tmp file ... [sock_uuid = '%s'; file_path = '%s']", sock_uuid, tmp_file_path)
		try:
			with open(tmp_file_path, 'wb') as f:
				for chunk in chunks:
					while True:
						try:
							f.write(chunk)
						except:
							logger.exception("Couldn\'t write chunk to .tmp file - will sleep for a bit and try again ... [sock_uuid = '%s'; file_path = '%s']", sock_uuid, tmp_file_path)
							time.sleep(FILE_ACCESS_ERROR_BACKOFF)
							continue
						break
		except:
			logger.exception("Couldn\'t open .tmp file for writing - will sleep for a bit and try again ... [sock_uuid = '%s'; file_path = '%s']", sock_uuid, tmp_file_path)
			time.sleep(FILE_ACCESS_ERROR_BACKOFF)
			continue
		logger.debug("Wrote .tmp file! [sock_uuid = '%s'; file_path = '%s']", sock_uuid, tmp_file_path)
		break

	while True:
		logger.debug("Renaming .tmp file ... [sock_uuid = '%s'; file_path = '%s']", sock_uuid, tmp_file_path)
		try:
			os.rename(tmp_file_path, file_path)
		except:
			logger.exception("Couldn\'t rename .tmp file - will sleep for a bit and try again ... [sock_uuid = '%s'; file_path = '%s']", sock_uuid, tmp_file_path)
			time.sleep(FILE_ACCESS_ERROR_BACKOFF)
			continue
		logger.debug("Renamed .tmp file! [sock_uuid = '%s'; file_path = '%s']", sock_uuid, file_path)
		break

files_to_delete = set()
files_to_delete_lock = Lock()
def file_deleter(started_unread_sock_uuids = None, started_unread_sock_uuids_lock = None):
	global files_to_delete
	while True:
		try:
			time.sleep(3)
			deleted_files = set()
			if len(files_to_delete) > 0:
				logger.debug('Deleting files queued for deletion ...')
				with files_to_delete_lock:
					tmp_files_to_delete = files_to_delete.copy()
				for file_path in tmp_files_to_delete:
					logger.debug("Deleting file queued for deletion ... [file_path = '%s']", file_path)
					try:
						os.remove(file_path)
					except FileNotFoundError:
						logger.debug("File queued for deletion doesn't exist, probably already deleted (harmless race condition between delete_file and delete_socket_files) - pressing on ... [file_path = '%s']", file_path)
					except:
						logger.exception("Failed to delete file queued for deletion - will try again in a bit [file_path = '%s']", file_path)
						continue
					else:
						logger.debug("Deleted file queued for deletion! [file_path = '%s']", file_path)
					deleted_files.add(file_path)
					if started_unread_sock_uuids is not None and started_unread_sock_uuids_lock is not None and file_path.endswith('.0.dat'):
						sock_uuid = os.path.basename(file_path)[:32]
						logger.debug("Removing UUID from the started but unread socket UUID thread set because its initial file (*.0.dat) has been read and queued for deletion ... [sock_uuid = '%s'; file_path = '%s']", sock_uuid, file_path)
						with started_unread_sock_uuids_lock:
							started_unread_sock_uuids.discard(sock_uuid)
			if len(deleted_files) > 0:
				with files_to_delete_lock:
					files_to_delete -= deleted_files
		except:
			logger.exception('Exception in file deleter loop - ignore and press on')

def delete_file(file_path):
	with files_to_delete_lock:
		files_to_delete.add(file_path)

def delete_socket_files(sock_uuid, dir_path):
	while True:
		try:
			dir_files = os.listdir(dir_path)
		except:
			logger.exception("Couldn\'t list dir - will sleep for a bit and try again ... [sock_uuid = '%s'; dir_path = '%s']", sock_uuid, dir_path)
			time.sleep(FILE_ACCESS_ERROR_BACKOFF)
			continue
		break

	file_name_prefix_to_delete = sock_uuid + '.'
	with files_to_delete_lock:
		for file_name in dir_files:
			if file_name.startswith(file_name_prefix_to_delete):
				files_to_delete.add(os.path.join(dir_path, file_name))

def clean_dir(dir_path):
	for file_name in os.listdir(dir_path):
		os.remove(os.path.join(dir_path, file_name))

# 8 bytes float time.time() + 16 bytes os.urandom = 24 bytes UUID
def get_uuid_bytes():
	return struct.pack('>d', time.clock()) + os.urandom(16)

# a 32 chars long url safe base64 ('+/' -> '-_') encoded string UUID
def get_uuid_base64():
	return base64.urlsafe_b64encode(get_uuid_bytes()).decode(encoding = 'utf-8')