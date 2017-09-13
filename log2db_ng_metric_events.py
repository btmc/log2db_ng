#!/usr/local/bin/python -Wignore::DeprecationWarning
# -*- coding: utf-8 -*-

from __future__ import print_function

import os
import sys
import glob
import re
import psycopg2
import time
import datetime
import optparse
import json
import tempfile
import collections
import traceback

from log2db_ng_field_types import *

pgsql_conn = None

def parse_args():
	usage = "usage: %prog [options] [file1, file2, ...]"
	description = "Process metric logs"

	parser = optparse.OptionParser(usage = usage, description = description)

	parser.add_option("-d", "--dir", 
				type    = "string", 
				dest    = "dir",
				default = "/tmp/metric_events",
				help    = "path to log files")
	
	parser.add_option("-t", "--type", 
				type    = "string", 
				dest    = "data_type",
				default = "views",
				help    = "data type")
	
	parser.add_option("-m", "--mask", 
				type    = "string", 
				dest    = "mask", 
				default = "*.yastng.*.log",
				help    = "wildcard for log files")

	parser.add_option("-i", "--limit", 
				type    = "string", 
				dest    = "limit",
				default = 2000,
				help    = "mask match limit")

	parser.add_option("-v", "--verbose", 
				action  = "store_true", 
				dest    = "verbose", 
				default = False,
				help    = "verbose output")

	parser.add_option("-H", "--dbhost", 
				type    = "string", 
				dest    = "dbhost", 
				default = "10.2.11.5",
				help    = "db host")

	parser.add_option("-U", "--dbuser", 
				type    = "string", 
				dest    = "dbuser", 
				default = "upload",
				help    = "db user")

	parser.add_option("-P", "--dbpassword", 
				type    = "string", 
				dest    = "dbpassword", 
				default = "DijUfAZi",
				help    = "db password")

	parser.add_option("-B", "--UPLOAD_SESSION", 
				type    = "string", 
				dest    = "dbdatabase", 
				default = "stats",
				help    = "db name")

	parser.add_option("-s", "--sampling_data", 
				type    = "string", 
				dest    = "sampling_data", 
				default = "u:", 	# example "cid:1234" 	=> 1) cid is mandatory 2) value of cid like "1234..."
									# example "cid:" 		=> 1) cid is mandatory 2) value does not matter
				help    = "sampling_data")	
  

	(prog_options, prog_args) = parser.parse_args()

	return (prog_options, prog_args)

class UploadSession(object):
	def __init__(self, filename, data_type, sampling_data, *args, **kwargs):
		global pgsql_conn
		self.pgsql_conn = pgsql_conn

		self.pgsql_conn_cursor = self.pgsql_conn.cursor()
		self.pgsql_conn_cursor.execute("set lock_timeout = '3s'")

		self.filename = filename 
		self.data_type = data_type
		
		self.sampling_data = sampling_data

		self.sampling_data_mask = ''
		self.sampling_data_mask_len = 0
		sampling_data_splited = sampling_data.split(':')
		self.sampling_data_field = sampling_data_splited[0] # re.split('[:]', sampling_data) # self.sampling_data = sampling_data
		if len(sampling_data_splited)>1:
			self.sampling_data_mask = sampling_data_splited[1]
			self.sampling_data_mask_len = len(self.sampling_data_mask)

		super(UploadSession, self).__init__(*args, **kwargs)

	def __enter__(self):
		for subclass in self.__class__.__subclasses__():
			if self.data_type in subclass.data_types:
				return subclass(self.filename, self.data_type, self.sampling_data) # return subclass(self.filename, self.data_type, self.sampling_data_field, self.sampling_data_mask) # 

		raise TypeError()

	def __exit__(self, *args, **kwargs):
		if type is not None:
			self.pgsql_conn.rollback()

		self.pgsql_conn_cursor.close()

	def open(self):
		self.pgsql_conn_cursor.execute  ( \
										   'insert into \
												upload_session    ( \
																	log_filename, \
																	data_type \
																  ) \
											values  ( \
														%s, \
														%s \
													) \
											returning \
												id', \
											( \
												os.path.basename(self.filename), \
												self.data_type \
											) \
										)

		return self.pgsql_conn_cursor.fetchone()[0]
 
	def parse_line(self, line): # , sampling_data_field, sampling_data_mask
		raise NotImplementedError()

	def parse(self):		
		self.session_id = self.open()
		log_file = open(self.filename, 'r')
		tmp_file = tempfile.TemporaryFile()
		error_file = tempfile.NamedTemporaryFile(dir=os.path.dirname(self.filename))
		#print(log_file)
		self.rows_processed = 0
		self.rows_prepared = 0

		for line in log_file:
			try:
				self.rows_processed += 1

				line = line.strip()

				facts = self.parse_line(line) # , self.sampling_data_field, self.sampling_data_mask)
				# facts = {k:v.clean() for k,v in facts.iteritems()}

				if facts != None:
					tmp_file.write('\t'.join((str(self.session_id), json.dumps(facts).encode('string-escape'),)) + '\n')

				self.rows_prepared += 1

				if not (self.rows_processed % 1000):
						sys.stdout.write('#')
						sys.stdout.flush()

			except Exception as e:
				print('\nException parsing row %s' %(self.rows_processed,))
				tb_last = sys.exc_traceback

				while tb_last.tb_next:
					tb_last = tb_last.tb_next

				field_info = None
				if 'self' in tb_last.tb_frame.f_locals:
					if isinstance(tb_last.tb_frame.f_locals['self'], LogField):
						field_info = '%s: %s' % (tb_last.tb_frame.f_locals['self'].__class__, tb_last.tb_frame.f_locals['self'].value,)

				print(field_info or traceback.format_exc())

				_ = map(lambda f: print(line, file=f), (sys.stdout, error_file,))
		
		tmp_file.seek(0)

		self.pgsql_conn_cursor.execute  ( \
											' \
												drop table if exists \
													%(data_type)s_upload_data_%(session_id)s \
											' % \
											{ \
												'data_type':    self.data_type, \
												'session_id':   self.session_id, \
											} \
										)

		self.pgsql_conn_cursor.execute  ( \
											' \
												create table \
													%(data_type)s_upload_data_%(session_id)s    ( \
																									like \
																										upload_data \
																								) \
											' % \
											{ \
												'data_type':    self.data_type, \
												'session_id':   self.session_id, \
											} \
										)

		self.pgsql_conn_cursor.copy_from( \
											tmp_file, \
											' \
												%(data_type)s_upload_data_%(session_id)s \
											' % \
											{ \
												'data_type':    self.data_type, \
												'session_id':   self.session_id, \
											} \
										)

		self.pgsql_conn_cursor.execute  ( \
											' \
												update \
													upload_session \
												set \
													rows_processed = %(rows_processed)s, \
													rows_prepared = %(rows_prepared)s, \
													ts_min = t1.ts_min, \
													ts_max = t1.ts_max, \
													dt = t1.dt \
												from \
													( \
														select \
															min((row#>>\'{ts}\')::int) ts_min, \
															max((row#>>\'{ts}\')::int) ts_max, \
															(timestamp \'epoch\' at time zone \'GMT\' + interval \'1 second\' * min((row#>>\'{ts}\')::int))::date dt \
														from \
															upload.%(data_type)s_upload_data_%(session_id)s \
													) t1 \
												where \
													id = %(session_id)s \
											' % \
											{ \
												'rows_processed':   self.rows_processed, \
												'rows_prepared':    self.rows_prepared, \
												'data_type':        self.data_type, \
												'session_id':       self.session_id, \
											} \
										)
		self.pgsql_conn.commit() 

		log_file.close()
		tmp_file.close()

		if error_file.tell():
		   os.link(error_file.name, '%s.%s.error' % (self.filename, self.session_id,)) 

		error_file.close()



class UploadSessionMetricEvents(UploadSession):
	data_types = ['metric_events', 'metric_events_test', 'metric_events_v5',]

	anonymous_fields_server = ('rts', 'ip',)

	def __init__(self, filename, data_type, sampling_data, *args, **kwargs):
		super(UploadSessionMetricEvents, self).__init__(filename, data_type, sampling_data, *args, **kwargs)

		self.pgsql_conn_cursor.execute  ( \
											' \
												select \
													field_from, \
													field_to, \
													field_type, \
													is_mandatory \
												from \
													upload.upload_session_field \
												where \
													data_type = \'%(data_type)s\' \
											' % \
											{ \
												'data_type':    self.data_type, \
											} \
										)

		self.fields = self.pgsql_conn_cursor.fetchall()
		
		self.mandatory_fields = set(x[0] for x in filter(lambda x: x[3], self.fields))


	def parse_line(self, line): # , sampling_data_field, sampling_data_mask
		line1 = line.lower()
		split1 = re.split('[|&]', line1)
		# print(split1)

		if len(split1)<3:
			print(line1)
			raise NameError('len(split1)<3') 
		elif len(split1)==3: # 1489616704.137|46.38.48.106|4.3%7C085049C7-8889-9F9F-1424-62F5B6E53B2B%7C%7C2A9328B8-BBCB-E4C1-3585-406573454A6A%7C5%7C132%7C%7CF%7CPLADF%7C%7C100821058%7C%7C%7Chttp://cdn.pladform.ru%7C%7C%7CM%7CPLDF_PL:13151%7CADXREF:http%3A%2F%2Fads.adfox.ru%2F239297%2FgetCode%3Fpp%3Dgiy%26ps%3Dcbgy%26p2%3Dfefs%26pfc%3Da%26pfb%3Da%26plp%3Da%26pli%3Da%26pop%3Da%26dl%3Dseasonvar.ru%26puid1%3D1%26puid22%3D16%26puid6%3D29%26puid33%3D3%26puid8%3D9%26puid11%3D0%26puid12%3D13%7CADVXL:0%7CADREF:nobanner%7CADVPT:124%7CADCLS:0%7CADWRPC:0
			line2 = urllib.unquote(line1).decode('utf8', 'replace').encode('utf8')
			split1 = line2.split('|')
			if len(split1)==3:
				print(line1)
				raise NameError('len(split1)=3') 

		fields = {}
		k=0
		for v in split1:
			field_name = ''
			field_value = ''							
			if k < len(self.anonymous_fields_server):
				field_name = self.anonymous_fields_server[k]
				field_value = v
			else:
				v1 = v.split("=", 1)
				if len(v1) == 2:
					field_name = v1[0]				
					field_value = v1[1] #, k 
				else:  # 1489616703.683|46.38.48.106|4.3%7C085049C7-8889-9F9F-1424-62F5B6E53B2B%7C%7C2A9328B8-BBCB-E4C1-3585-406573454A6A%7C5%7C129%7C%7CF%7CPLADF%7C%7C100821058%7C%7C%7Chttp://cdn.pladform.ru%7C%7C%7CM%7CADWRPC:0%7CADREF:nobanner%7CADVPT:101%7CADVXL:0%7CADXREF:http%3A%2F%2Fad.mail.ru%2Fvast%2F3930%3Fpuid1%3D13%26puid2%3D301%26puid3%3D2%26puid4%3D1%26puid5%3D16%26puid6%3D29%26puid7%3D3%26puid8%3D9%26puid9%3D1%26puid10%3D1%26puid11%3D0%26puid12%3D13%26dl%3Dhttp%253A%252F%252Fseasonvar.ru%252Fserial-6625-Kohanya-02-sezon.html%26duration%3D1463%26content_id%3D100821058%26eid2%3D13151%26puid18%3D1%26eid1%3D13151%7C109%7C100821058%7C2028%7CADCLS:0%7CPLDF_PL:13151
					field_name = 'err_log_unknown'
					if fields.has_key(field_name):
						field_value = fields[field_name] + '|' + v1[0]
					else:
						field_value = v1[0] 
			fields[field_name] = field_value #, k # fields.update(field_name, field_value)			
			k = k + 1
		assert self.mandatory_fields == self.mandatory_fields & set(fields.keys()) , "no_mandatory_fields"

		def process_fields():
			facts = {}
			sampling_finded = False	# sampling_passed = True	
			# facts['log_source'] = 0 # TODO: допилить с учетом yast/metric 						 
			for field in fields: # поля лога		
				field_from = field				
				if field_from == self.sampling_data_field:
					sampling_finded = True
					if self.sampling_data_mask_len > 0:
						if self.sampling_data_mask != fields[field_from][:self.sampling_data_mask_len]:
							return None # sampling_passed = False # None
				field_to = ''
				for row in self.fields: # справочник с маппингом
					if row[0]==field_from:
						field_to = row[1]
						field_type = row[2]
						try:
							v = (eval('%s(fields[\'%s\'])' % (field_type, field_from, ))).clean()
							facts[field_to] = v # eval('%s(fields[\'%s\'])' % (field_type, field_from,)).clean()
						except:
							bad_data = field_from + ":" + fields[field_from]
							if facts.has_key('err_log_bad'):
								facts['err_log_bad'] = facts['err_log_bad'] + '|' + bad_data
							else:
								facts['err_log_bad'] = bad_data
							print('err_log_bad: ' + facts['err_log_bad'])
						continue # (!) одно и тоже поле может маппиться под разными соусами (например, по ip - геоданные)   
				# print(fields)
				if field_to=='' and field_from != 'err_log_unknown':
					if facts.has_key('err_log_unknown'):
						facts['err_log_unknown'] = facts['err_log_unknown'] + '|' + field_from
					else:
						facts['err_log_unknown'] = field_from
			# print("fields:")
			# print(fields)
			# print("facts:")
			# print(facts)
			assert sampling_finded, 'no_' + self.sampling_data_field
			return facts

		return process_fields()	
		

def main():
	(prog_options, prog_args) = parse_args()

	global pgsql_conn
	pgsql_conn = psycopg2.connect(**{i.replace('db',''):j for i,j in vars(prog_options).iteritems() if re.match('db', i)})

	if prog_args:
		log_filenames = prog_args
	else:
		log_filenames = sorted(glob.glob(os.path.join(prog_options.dir, prog_options.mask)))[:prog_options.limit]

	print('Processing %s files' % (len(log_filenames),))
	
	for log_filename in log_filenames:
		with UploadSession(log_filename, prog_options.data_type, prog_options.sampling_data) as log_session:
			time_start = time.time()
			print('Started processing file %s, sampling_data - %s' % (log_filename, prog_options.sampling_data))

			log_session.parse()

			print('\nFinished processing file %s in %s - %s of %s rows processed' %(log_filename, str(datetime.timedelta(seconds = int(time.time() - time_start))), log_session.rows_prepared, log_session.rows_processed,))

		os.unlink(log_filename)             

	pgsql_conn.close()

if __name__ == "__main__":
	main()