import socket
import sys
import time
import threading


# returns the status code from a given HTTP response message
def get_status_code(response):
	response_lines = response.split("\r\n")
	status_line = response_lines[0]
	stat_code_phrase = status_line[status_line.find(" ")+1:]
	return stat_code_phrase

# returns the smallest integer that is bigger than length and is a power of 2
def ceil_power_2(length):
	ceil_pow = 4096
	while ceil_pow < length:
		ceil_pow *= 2

	ceil_pow *= 2
	return ceil_pow


# returns the content length from a given HTTP response message
def get_content_length(response):
	response_lines = response.split("\r\n")
	for line in response_lines:
		if line.split(' ')[0] == "Content-Length:":
			content_length = int(line.split(' ')[1])
			return content_length
	return 0


# returns the buffer size from a given HTTP response message
def find_buffer_size(response):
	content_length = get_content_length(response)
	buf_size = ceil_power_2(content_length)
	# print("Content length: {}, Buffer size: {}".format(content_length, buf_size))
	return buf_size


# returns content range from a given HTTP response message
def get_content_range(response):
	response_lines = response.split("\r\n")
	for line in response_lines:
		if line.split(' ')[0] == "Content-Range:":
			content_range = line.split(' ')[2]
			lower_range, upper_range = content_range.split('-')
			upper_range = upper_range.split('/')[0]
			return int(lower_range), int(upper_range)
	return 0, 0


# returns directory substring from a given URL
def get_directory(url):
	return url[url.find('/'):]


# returns the object from a response message
def get_object(response):
	idx = response.find("\r\n\r\n")
	if idx == -1:
		return ""
	return response[idx+4:]

# return the all parts of a response message combined
def recv_all(sock, response):
	timeout = 1
	sock.setblocking(False)
	data = bytearray()
	begin = time.time()
	content_length = get_content_length(response)
	while True:
		if data and time.time() - begin > timeout:
			break
		elif time.time() - begin > timeout * 2:
			break
		try:
			packet = sock.recv(8192)
			if packet:
				data.extend(packet)
				begin = time.time()
			else:
				time.sleep(0.2)
		except:
			pass
		if len(get_object(data.decode())) == content_length:
			break
	return data

# download & return part of the file specified by lrange-urange, located in url
# idx and lock are related to multithreading, idx is the index of the thread, lock is to avoid race conditions
def download_file_part(response, lrange, urange, url, idx, lock):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	host_url = url.split("/")[0]
	sock.connect((host_url, 80))

	directory = url[url.find('/'):]

	request = "GET {} HTTP/1.1\r\nHost: {}\r\nRange: bytes={}-{}\r\n\r\n".format(directory, host_url, lrange, urange)
	sock.sendall(request.encode())

	n = (urange - lrange + 1)
	content_length = get_content_length(response)
	if content_length < (urange - lrange + 1):
		n = content_length - lrange

	timeout = 1
	sock.setblocking(False)
	data = bytearray()
	begin = time.time()
	while True:
		if data and time.time() - begin > timeout:
			break
		elif time.time() - begin > timeout * 2:
			break
		try:
			packet = sock.recv(8192)
			if packet:
				data.extend(packet)
				begin = time.time()
			else:
				time.sleep(0.2)
		except:
			pass
		if len(get_object(data.decode())) == n:
			break
	
	sock.shutdown(socket.SHUT_RDWR)
	sock.close()

	lock.acquire()
	global downloaded_file_parts

	downloaded_file_parts[idx] = get_object(data.decode()) # decode the received data before appending it to the file parts array
	lock.release()


# updates the thread_download_ranges with the correct boundaries
# n is the content length, and k is the connection_cnt/parts to divide
def get_thread_ranges(n, thread_download_ranges, k):
	if n % k == 0:
		current_end = 0
		for download_range in thread_download_ranges:
			download_range[0] = current_end
			download_range[1] = current_end + (n // k) - 1
			current_end += n // k
	else:
		current_end = 0
		for connection_idx, download_range in enumerate(thread_download_ranges, 1):
			if connection_idx <= (n - (n // k) * k):
				download_range[0] = current_end
				download_range[1] = current_end + (n // k)
				current_end += (n // k) + 1
			else:
				download_range[0] = current_end
				download_range[1] = current_end + (n // k) - 1
				current_end += n // k


# start main
if len(sys.argv) != 3:
	print("Incorrect # of arguments")
	sys.exit()

index_file = sys.argv[1]
connection_cnt = int(sys.argv[2])

# get host URL from the index URL
host_url = index_file.split("/")[0]
print("URL of the index file: {}".format(index_file))

print("Number of parallel connections: {}".format(connection_cnt))

# instantiate the socket and connect to host
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.connect((host_url, 80))

directory = get_directory(index_file)
# print("directory:", directory)

# add HEAD request here

# send an additional HEAD request to determine the buffer size for the object
head_request = "HEAD {} HTTP/1.1\r\nHost: {}\r\n\r\n".format(directory, host_url)

s.sendall(head_request.encode())

head_response = s.recv(16384).decode()

# check the status code
stat_code_phrase = get_status_code(head_response)

if stat_code_phrase != "200 OK":
	print("Error")
	print("Status line:", stat_code_phrase)
	print("Exiting!")
	sys.exit()


buffer_size = find_buffer_size(head_response)

# send GET request to retrieve the index file
request = "GET {} HTTP/1.1\r\nHost: {}\r\n\r\n".format(directory, host_url)

# print("request:", request)

s.sendall(request.encode())

response = recv_all(s, head_response).decode()
# response = s.recv(buffer_size).decode()

print("Index file is downloaded")

# print("Response")
# print(response)

# check the status code
stat_code_phrase = get_status_code(response)

if stat_code_phrase != "200 OK":
	print("Error")
	print("Status line:", stat_code_phrase)
	print("Exiting!")
	sys.exit()

response_lines = response.split("\r\n")

# print("Response lines")
# print(response_lines)

# get file URLs from the index file
file_urls = response_lines[-1].split("\n")
file_urls = [url for url in file_urls if len(url) != 0]

# print("File URLs")
# print(repr(file_urls))
print("There are {} files in the index".format(len(file_urls)))

# close the initial socket
s.shutdown(socket.SHUT_RDWR)
s.close()

for idx, url in enumerate(file_urls, 1):
	# define the variables range(connection_cnt)
	downloaded_file_parts = [''] * connection_cnt # parts of the downloaded file will be saved here
	thread_download_ranges = []
	for i in range(connection_cnt):
		thread_download_ranges.append([0,0])
	
	# create new socket for each URL in the index file
	file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	host_url = url.split("/")[0]
	filename = url.split("/")[-1]

	file_socket.connect((host_url, 80))

	directory = get_directory(url)

	# send HEAD request to check the range constraints
	request = "HEAD {} HTTP/1.1\r\nHost: {}\r\n\r\n".format(directory, host_url)
	file_socket.sendall(request.encode())

	response = file_socket.recv(16384).decode()
	stat_code_phrase = get_status_code(response)


	# check the status code
	if stat_code_phrase != "200 OK":
		print("{}. {} is not found".format(idx, url))
		continue

	# close the file socket
	file_socket.shutdown(socket.SHUT_RDWR)
	file_socket.close()

	# determine the buffer size
	content_length = get_content_length(response)

	get_thread_ranges(content_length, thread_download_ranges, connection_cnt)

	threads = []
	lock = threading.Lock() # lock for avoiding race conditions

	# create the connection_cnt many threads
	for connection in range(connection_cnt):
		# each thread executes the download_file_part function with the specified parameters
		cur_thread = threading.Thread(target=download_file_part, args=(response, thread_download_ranges[connection][0], thread_download_ranges[connection][1], url, connection, lock))
		threads.append(cur_thread)
		cur_thread.start()

	# wait for all threads to finish & terminate them
	for index, thread in enumerate(threads, 1):
		thread.join()

	# combine downloaded parts
	downloaded_content = ''.join(downloaded_file_parts)
	
	# print the relevant output on the console
	print("{}. {} (size = {}) is downloaded".format(idx, url, content_length))
	
	file_parts = 'File parts: '
	for down_range in thread_download_ranges:
		file_parts += str(down_range[0]) + ":" + str(down_range[1]) + "(" + str(down_range[1] - down_range[0] + 1) + "), "

	file_parts = file_parts[:-2] # remove the last , and space
	print(file_parts)

	# write the object to a file
	with open(filename, "w") as file:
		file.write(downloaded_content)