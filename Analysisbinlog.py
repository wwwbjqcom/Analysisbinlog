#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@author: Great God
'''

import struct,time,datetime,os
import sys,decimal,getopt,types
reload(sys)
sys.setdefaultencoding('utf-8')
'''column type '''
class column_type_dict:
    MYSQL_TYPE_DECIMAL=0
    MYSQL_TYPE_TINY=1
    MYSQL_TYPE_SHORT=2
    MYSQL_TYPE_LONG=3
    MYSQL_TYPE_FLOAT=4
    MYSQL_TYPE_DOUBLE=5
    MYSQL_TYPE_NULL=6
    MYSQL_TYPE_TIMESTAMP=7
    MYSQL_TYPE_LONGLONG=8
    MYSQL_TYPE_INT24=9
    MYSQL_TYPE_DATE=10
    MYSQL_TYPE_TIME=11
    MYSQL_TYPE_DATETIME=12
    MYSQL_TYPE_YEAR=13
    MYSQL_TYPE_NEWDATE=14
    MYSQL_TYPE_VARCHAR=15
    MYSQL_TYPE_BIT=16
    MYSQL_TYPE_TIMESTAMP2=17
    MYSQL_TYPE_DATETIME2=18
    MYSQL_TYPE_TIME2=19
    MYSQL_TYPE_JSON=245
    MYSQL_TYPE_NEWDECIMAL=246
    MYSQL_TYPE_ENUM=247
    MYSQL_TYPE_SET=248
    MYSQL_TYPE_TINY_BLOB=249
    MYSQL_TYPE_MEDIUM_BLOB=250
    MYSQL_TYPE_LONG_BLOB=251
    MYSQL_TYPE_BLOB=252
    MYSQL_TYPE_VAR_STRING=253
    MYSQL_TYPE_STRING=254
    MYSQL_TYPE_GEOMETRY=255

''''''

'''mysql_binlog_event start GTID_EVENT end XID_EVENT'''
class binlog_events:
    UNKNOWN_EVENT= 0
    START_EVENT_V3= 1
    QUERY_EVENT= 2
    STOP_EVENT= 3
    ROTATE_EVENT= 4
    INTVAR_EVENT= 5
    LOAD_EVENT= 6
    SLAVE_EVENT= 7
    CREATE_FILE_EVENT= 8
    APPEND_BLOCK_EVENT= 9
    EXEC_LOAD_EVENT= 10
    DELETE_FILE_EVENT= 11
    NEW_LOAD_EVENT= 12
    RAND_EVENT= 13
    USER_VAR_EVENT= 14
    FORMAT_DESCRIPTION_EVENT= 15
    XID_EVENT= 16
    BEGIN_LOAD_QUERY_EVENT= 17
    EXECUTE_LOAD_QUERY_EVENT= 18
    TABLE_MAP_EVENT = 19
    PRE_GA_WRITE_ROWS_EVENT = 20
    PRE_GA_UPDATE_ROWS_EVENT = 21
    PRE_GA_DELETE_ROWS_EVENT = 22
    WRITE_ROWS_EVENT = 23
    UPDATE_ROWS_EVENT = 24
    DELETE_ROWS_EVENT = 25
    INCIDENT_EVENT= 26
    HEARTBEAT_LOG_EVENT= 27
    IGNORABLE_LOG_EVENT= 28
    ROWS_QUERY_LOG_EVENT= 29
    WRITE_ROWS_EVENT = 30
    UPDATE_ROWS_EVENT = 31
    DELETE_ROWS_EVENT = 32
    GTID_LOG_EVENT= 33
    ANONYMOUS_GTID_LOG_EVENT= 34
    PREVIOUS_GTIDS_LOG_EVENT= 35
''''''

'''json type'''
class json_type:
    NULL_COLUMN = 251
    UNSIGNED_CHAR_COLUMN = 251
    UNSIGNED_SHORT_COLUMN = 252
    UNSIGNED_INT24_COLUMN = 253
    UNSIGNED_INT64_COLUMN = 254
    UNSIGNED_CHAR_LENGTH = 1
    UNSIGNED_SHORT_LENGTH = 2
    UNSIGNED_INT24_LENGTH = 3
    UNSIGNED_INT64_LENGTH = 8

    JSONB_TYPE_SMALL_OBJECT = 0x0
    JSONB_TYPE_LARGE_OBJECT = 0x1
    JSONB_TYPE_SMALL_ARRAY = 0x2
    JSONB_TYPE_LARGE_ARRAY = 0x3
    JSONB_TYPE_LITERAL = 0x4
    JSONB_TYPE_INT16 = 0x5
    JSONB_TYPE_UINT16 = 0x6
    JSONB_TYPE_INT32 = 0x7
    JSONB_TYPE_UINT32 = 0x8
    JSONB_TYPE_INT64 = 0x9
    JSONB_TYPE_UINT64 = 0xA
    JSONB_TYPE_DOUBLE = 0xB
    JSONB_TYPE_STRING = 0xC
    JSONB_TYPE_OPAQUE = 0xF

    JSONB_LITERAL_NULL = 0x0
    JSONB_LITERAL_TRUE = 0x1
    JSONB_LITERAL_FALSE = 0x2
''''''

BINLOG_FILE_HEADER = b'\xFE\x62\x69\x6E'
binlog_event_header_len = 19
binlog_event_fix_part = 13
binlog_quer_event_stern = 4
binlog_row_event_extra_headers = 2
read_format_desc_event_length = 56
binlog_xid_event_length = 8
table_map_event_fix_length = 8
fix_length = 8


''''''
class _at_pos:
    _at_pos = None
    _next_pos = None

''''''
class _rollback:
    rollback_status = None

    database = None
    table = None
    _gtid = None

    _myfile = None
    _myfunc = None
''''''

''''''
class _remote_filed:
    _gtid = None
    _gtid_status = None

    _thread_id = None
    _tid_status = None
    _tid_gid = None
    _tid_gid_pos = None
    _tid_gid_time = None

    _rollback_status = None
''''''


class Echo(object):
    '''
    print binlog 
    '''

    def Version(self, binlog_ver, server_ver, create_time):
        print 'binlog_ver : {}   server_ver : {}   create_time : {}'.format(binlog_ver, server_ver, create_time)


    def TractionHeader(self, thread_id, database_name, sql_statement, timestamp,_pos):

        if _remote_filed._thread_id:
            if _remote_filed._thread_id == thread_id:
                self.Gtid(timestamp,_remote_filed._tid_gid,_remote_filed._tid_gid_pos)
                print '{}  GTID_NEXT : {} at pos : {}'.format(_remote_filed._tid_gid_time, _remote_filed._tid_gid, _remote_filed._tid_gid_pos)
                print '{}  thread id : {}  at pos : {}  database : {}   statement : {}'.format(timestamp, thread_id, _pos,
                                                                                               database_name, sql_statement)
                _remote_filed._tid_status = True
                if _remote_filed._rollback_status:
                    _rollback.database = database_name
        elif _remote_filed._gtid:
            if _remote_filed._gtid_status:
                print '{}  thread id : {}  at pos : {}  database : {}   statement : {}'.format(timestamp, thread_id,
                                                                                               _pos,
                                                                                               database_name,
                                                                                               sql_statement)
                if _remote_filed._rollback_status:
                    _rollback.database = database_name
        elif _rollback.rollback_status:
            _rollback.database = database_name
        else:
            print '{}  thread id : {}  at pos : {}  database : {}   statement : {}'.format(timestamp, thread_id, _pos, database_name, sql_statement)

    def Xid(self, timestamp, xid_num,_pos):
        if _remote_filed._thread_id:
            if _remote_filed._tid_status:
                _remote_filed._tid_status = None
                print '{}  statement : COMMIT  xid : {}  at pos : {}'.format(timestamp, xid_num, _pos)
                print ''
        elif _remote_filed._gtid:
            if _remote_filed._gtid_status:
                print '{}  statement : COMMIT  xid : {}  at pos : {}'.format(timestamp, xid_num, _pos)
                print ''
                raise ''
        elif _rollback.rollback_status:
            _rollback._myfunc.SaveGtid(xid=True)
        else:
            print '{}  statement : COMMIT  xid : {}  at pos : {}'.format(timestamp, xid_num, _pos)
            print ''

    def Tablemap(self, timestamp, tablename):
        if _remote_filed._thread_id:
            if _remote_filed._tid_status:
                print '{}  tablename : {}'.format(timestamp, tablename)
            if _remote_filed._rollback_status:
                _rollback.table = tablename
        elif _remote_filed._gtid:
            if _remote_filed._gtid_status:
                print '{}  tablename : {}'.format(timestamp, tablename)
            if _remote_filed._rollback_status:
                _rollback.table = tablename
        elif _rollback.rollback_status:
            _rollback.table = tablename
        else:
            print '{}  tablename : {}'.format(timestamp, tablename)
    def Gtid(self, timestamp, gtid,_pos):
        if _remote_filed._thread_id:
            _remote_filed._tid_gid,_remote_filed._tid_gid_pos,_remote_filed._tid_gid_time = gtid,_pos,timestamp
        elif _remote_filed._gtid:
            if _remote_filed._gtid == gtid:
                print '{}  GTID_NEXT : {} at pos : {}'.format(timestamp, gtid, _pos)
                _remote_filed._gtid_status = True
        elif _rollback.rollback_status:
            _rollback._myfunc.SaveGtid(gtid=gtid)
        else:
            print '{}  GTID_NEXT : {} at pos : {}'.format(timestamp, gtid,_pos)

    def TractionVlues(self, before_value=None, after_value=None, type=None):
        if _remote_filed._thread_id:
            if _remote_filed._tid_status:
                if _remote_filed._rollback_status:
                    _rollback._myfunc.CreateSQL(before_value=before_value, after_value=after_value, event_type=type)
                else:
                    self._tv(before_value=before_value, after_value=after_value, type=type)
        elif _remote_filed._gtid:
            if _remote_filed._gtid_status:
                if _remote_filed._rollback_status:
                    _rollback._myfunc.CreateSQL(before_value=before_value, after_value=after_value, event_type=type)
                else:
                    self._tv(before_value=before_value, after_value=after_value, type=type)
        elif _rollback.rollback_status:
            _rollback._myfunc.CreateSQL(before_value=before_value,after_value=after_value,event_type=type)
        else:
            self._tv(before_value=before_value,after_value=after_value,type=type)

    def _tv(self,before_value=None, after_value=None, type=None):
        if type == binlog_events.UPDATE_ROWS_EVENT:
            print '{: >21}{}  before_value : [{}]  after_value : [{}]'.format('', 'UPDATE_ROWS_EVENT',
                                                                              ','.join(['{}'.format(a) for a in
                                                                                        before_value]), ','.join(
                    ['{}'.format(a) for a in after_value]))
        else:
            if type == binlog_events.DELETE_ROWS_EVENT:
                print '{: >21}{}  value : [{}] '.format('', 'DELETE_ROW_EVENT',
                                                        ','.join(['{}'.format(a) for a in after_value]))
            elif type == binlog_events.WRITE_ROWS_EVENT:
                print '{: >21}{}  value : [{}] '.format('', 'WRITE_ROW_EVENT',
                                                        ','.join(['{}'.format(a) for a in after_value]))

class PrintSql(object):
    def __seek(self,num):
        try:
            _rollback._myfile.seek(-num,1)
        except:
            _rollback._myfile.close()
            self.rmfile()
            sys.exit()
    def read(self):
        _num = 9
        _rollback._myfile.seek(0,2)
        while True:
            self.__seek(_num)
            _value,_type_code, = struct.unpack('QB',_rollback._myfile.read(_num))
            self.__seek(_num)
            if _type_code == 1:
                self.__gtid(_value)
            elif _type_code == 2:
                self.__statement(_value)
            else:
                print 'Error: type_code {} '.format(_type_code)
    def __gtid(self,tid):
        self.__seek(36)
        _uuid = _rollback._myfile.read(36)
        gtid = _uuid.decode('utf8') + ':' + str(tid)
        print ''
        print '#{: >10}  GTID_NEXT : {}'.format('-',gtid)
        self.__seek(36)
    def __statement(self,length):
        self.__seek(length)
        _sql = _rollback._myfile.read(length)
        sql, = struct.unpack('{}s'.format(length),_sql)
        print '{: >10}{}'.format('',sql)
        self.__seek(length)

    def rmfile(self):
        os.remove('tmp_rollback')

class GetRollStatement(object):
    def __init__(self,host,user,passwd,port=None):
        import pymysql,traceback
        self.port = port if port != None else 3306
        try:
            self.local_conn = pymysql.connect(host=host, user=user, passwd=passwd, port=port, db='',
                                         charset="utf8")
        except pymysql.Error,e:
            print traceback.format_exc()
        self.column_list = []
        self._is_pri = []
    def __join(self,column,value):
        if type(value) is types.StringType:
            if value == 'Null':
                return ' {}={}'.format(column, value)
            else:
                return ' {}="{}"'.format(column, value)
        else:
            return  ' {}={}'.format(column,value)
    def WriteEvent(self,values):
        sql = 'delete from {}.{} where '.format(_rollback.database,_rollback.table)
        if self._is_pri:
            sql += self.__join(self._is_pri[0][0],values[self._is_pri[0][1]])
        else:
            for i,column in enumerate(self.column_list):
                sql += self.__join(column[0],values[i])
                if column != self.column_list[-1]:
                    sql += ' and'
        if _remote_filed._rollback_status:
            print '{: >21}{}{}'.format('', '-- ', sql)
        else:
            self.__tmppack(sql,2)
    def DeleteEvent(self,values):
        sql = 'insert into {}.{}({}) values('.format(_rollback.database,_rollback.table,','.join([a[0] for a in self.column_list]))
        for idex,value in enumerate(values):
            if type(value) is types.StringType:
                if value == 'Null':
                    sql += '{}'.format(value)
                else:
                    sql += '"{}"'.format(value)
            else:
                sql += '{}'.format(value)
            if len(values[idex:]) <= 1:
                sql += ')'
            else:
                sql += ','
        if _remote_filed._rollback_status:
            print '{: >21}{}{}'.format('', '-- ', sql)
        else:
            self.__tmppack(sql, 2)
    def UpateEvent(self,after_values,befor_values):
        _set = []
        _where = []
        if self._is_pri:
            _where.append(self.__join(self._is_pri[0][0],after_values[self._is_pri[0][1]]))
        else:
            for i,column in enumerate(self.column_list):
                _where.append(self.__join(column[0],after_values[i]))

        for i,column in enumerate(self.column_list):
            _set.append(self.__join(column[0],befor_values[i]))
        sql = 'update {}.{} set {} where {}'.format(_rollback.database, _rollback.table, ','.join(_set).replace(" ",""), ','.join(_where))
        if _remote_filed._rollback_status:
            print '{: >21}{}{}'.format('', '-- ',sql)
        else:
            self.__tmppack(sql, 2)

    def GetColumnName(self):
        with self.local_conn.cursor() as cur:
            sql = 'desc {}.{};'.format(_rollback.database,_rollback.table)
            cur.execute(sql)
            result = cur.fetchall()
        self.column_list = [[a[0],a[3]] for a in result]

    def CreateSQL(self,before_value=None,after_value=None,event_type=None):
        self.GetColumnName()
        self._is_pri = [[_a[0], idex] for idex, _a in enumerate(self.column_list) if 'PRI' in _a]
        if event_type == binlog_events.WRITE_ROWS_EVENT:
            self.WriteEvent(after_value)
        elif event_type == binlog_events.UPDATE_ROWS_EVENT:
            self.UpateEvent(after_value,before_value)
        elif event_type == binlog_events.DELETE_ROWS_EVENT:
            self.DeleteEvent(after_value)

    def SaveGtid(self,gtid=None,xid=None):
        if xid:
            __gtid = _rollback._gtid.split(':')
            tid = int(__gtid[1])
            uuid = str(__gtid[0])
            self.__tmppackgtid(uuid,tid,1)
        elif _rollback._gtid != gtid:
            _rollback._gtid = gtid

    def __tmppackgtid(self,uuid,tid,type):
        s_uuid = struct.Struct('{}s'.format(len(uuid)))
        s_header = struct.Struct('QB')
        _uuid = s_uuid.pack(uuid)
        _header = s_header.pack(tid,type)
        _rollback._myfile.write(_uuid)
        _rollback._myfile.write(_header)
    def __tmppack(self,value,type):
        import re
        _value = re.sub(r"\s{2,}"," ",str(value).strip()) + ';'
        s_value = struct.Struct('{}s'.format(len(_value)))
        s_header = struct.Struct('QB')
        _value = s_value.pack(_value)
        _header = s_header.pack(len(_value),type)
        _rollback._myfile.write(_value)
        _rollback._myfile.write(_header)

    def _close(self):
        self.local_conn.close()

class Read(Echo):
    def __init__(self,start_position=None,filename=None,pack=None):
        self.__packet = pack
        if filename:
            self.file_data = open(filename, 'rb')
            read_byte = self.read_bytes(4)
            if read_byte != BINLOG_FILE_HEADER:
                print 'error : Is not a standard binlog file format'
                exit()
        if start_position:
            self.file_data.seek(start_position-4,1)

    def read_int_be_by_size(self, size ,bytes=None):
        '''Read a big endian integer values based on byte number'''
        if bytes is None:
            if size == 1:
                return struct.unpack('>b', self.read_bytes(size))[0]
            elif size == 2:
                return struct.unpack('>h', self.read_bytes(size))[0]
            elif size == 3:
                return self.read_int24_be()
            elif size == 4:
                return struct.unpack('>i', self.read_bytes(size))[0]
            elif size == 5:
                return self.read_int40_be()
            elif size == 8:
                return struct.unpack('>l', self.read_bytes(size))[0]
        else:
            '''used for read new decimal'''
            if size == 1:
                return struct.unpack('>b', bytes[0:size])[0]
            elif size == 2:
                return struct.unpack('>h', bytes[0:size])[0]
            elif size == 3:
                return self.read_int24_be(bytes)
            elif size == 4:
                return struct.unpack('>i',bytes[0:size])[0]

    def read_int24_be(self,bytes=None):
        if bytes is None:
            a, b, c = struct.unpack('BBB', self.read_bytes(3))
        else:
            a, b, c = struct.unpack('BBB', bytes[0:3])
        res = (a << 16) | (b << 8) | c
        if res >= 0x800000:
            res -= 0x1000000
        return res

    def read_uint_by_size(self, size):
        '''Read a little endian integer values based on byte number'''
        if size == 1:
            return self.read_uint8()
        elif size == 2:
            return self.read_uint16()
        elif size == 3:
            return self.read_uint24()
        elif size == 4:
            return self.read_uint32()
        elif size == 5:
            return self.read_uint40()
        elif size == 6:
            return self.read_uint48()
        elif size == 7:
            return self.read_uint56()
        elif size == 8:
            return self.read_uint64()

    def read_uint24(self):
        a, b, c = struct.unpack("<BBB", self.read_bytes(3))
        return a + (b << 8) + (c << 16)

    def read_int24(self):
        a, b, c = struct.unpack("<bbb", self.read_bytes(3))
        return a + (b << 8) + (c << 16)

    def read_uint40(self):
        a, b = struct.unpack("<BI", self.read_bytes(5))
        return a + (b << 8)

    def read_int40_be(self):
        a, b = struct.unpack(">IB", self.read_bytes(5))
        return b + (a << 8)

    def read_uint48(self):
        a, b, c = struct.unpack("<HHH", self.read_bytes(6))
        return a + (b << 16) + (c << 32)

    def read_uint56(self):
        a, b, c = struct.unpack("<BHI", self.read_bytes(7))
        return a + (b << 8) + (c << 24)

    def read_bytes(self, count):
        try:
            return self.file_data.read(count) if self.__packet is None else self.__packet.read(count)
        except:
            return None

    def read_uint64(self):
        read_byte = self.read_bytes(8)
        result, = struct.unpack('Q', read_byte)
        return result

    def read_int64(self):
        read_byte = self.read_bytes(8)
        result, = struct.unpack('q', read_byte)
        return result

    def read_uint32(self):
        read_byte = self.read_bytes(4)
        result, = struct.unpack('I', read_byte)
        return result

    def read_int32(self):
        read_byte = self.read_bytes(4)
        result, = struct.unpack('i', read_byte)
        return result

    def read_uint16(self):
        read_byte = self.read_bytes(2)
        result, = struct.unpack('H', read_byte)
        return result

    def read_int16(self):
        read_byte = self.read_bytes(2)
        result, = struct.unpack('h', read_byte)
        return result

    def read_uint8(self):
        read_byte = self.read_bytes(1)
        result, = struct.unpack('B', read_byte)
        return result

    def read_int8(self):
        read_byte = self.read_bytes(1)
        result, = struct.unpack('b', read_byte)
        return result

    def read_format_desc_event(self):
        binlog_ver, = struct.unpack('H',self.read_bytes(2))
        server_ver, = struct.unpack('50s',self.read_bytes(50))
        create_time, = struct.unpack('I',self.read_bytes(4))
        return binlog_ver,server_ver,create_time

    def __add_fsp_to_time(self, time, column):
        """Read and add the fractional part of time
        For more details about new date format:
        """
        microsecond,read = self.__read_fsp(column)
        if microsecond > 0:
            time = time.replace(microsecond=microsecond)
        return time,read

    def __read_fsp(self, column):
        read = 0
        if column == 1 or column == 2:
            read = 1
        elif column == 3 or column == 4:
            read = 2
        elif column == 5 or column == 6:
            read = 3
        if read > 0:
            microsecond = self.read_int_be_by_size(read)
            if column % 2:
                return int(microsecond / 10),read
            else:
                return microsecond,read

        return 0,0


    def __read_binary_slice(self, binary, start, size, data_length):
        """
        Read a part of binary data and extract a number
        binary: the data
        start: From which bit (1 to X)
        size: How many bits should be read
        data_length: data size
        """
        binary = binary >> data_length - (start + size)
        mask = ((1 << size) - 1)
        return binary & mask

    def __read_datetime2(self, column):
        """DATETIME

        1 bit  sign           (1= non-negative, 0= negative)
        17 bits year*13+month  (year 0-9999, month 0-12)
         5 bits day            (0-31)
         5 bits hour           (0-23)
         6 bits minute         (0-59)
         6 bits second         (0-59)
        ---------------------------
        40 bits = 5 bytes
        """
        data = self.read_int_be_by_size(5)
        year_month = self.__read_binary_slice(data, 1, 17, 40)
        try:
            t = datetime.datetime(
                year=int(year_month / 13),
                month=year_month % 13,
                day=self.__read_binary_slice(data, 18, 5, 40),
                hour=self.__read_binary_slice(data, 23, 5, 40),
                minute=self.__read_binary_slice(data, 28, 6, 40),
                second=self.__read_binary_slice(data, 34, 6, 40))
        except ValueError:
            return None
        __time,read = self.__add_fsp_to_time(t, column)
        return __time,read

    def __read_time2(self, column):
        """TIME encoding for nonfractional part:

         1 bit sign    (1= non-negative, 0= negative)
         1 bit unused  (reserved for future extensions)
        10 bits hour   (0-838)
         6 bits minute (0-59)
         6 bits second (0-59)
        ---------------------
        24 bits = 3 bytes
        """
        data = self.read_int_be_by_size(3)

        sign = 1 if self.__read_binary_slice(data, 0, 1, 24) else -1
        if sign == -1:
            '''
            negative integers are stored as 2's compliment
            hence take 2's compliment again to get the right value.
            '''
            data = ~data + 1

        microseconds,read = self.__read_fsp(column)
        t = datetime.timedelta(
            hours=sign*self.__read_binary_slice(data, 2, 10, 24),
            minutes=self.__read_binary_slice(data, 12, 6, 24),
            seconds=self.__read_binary_slice(data, 18, 6, 24),
            microseconds= microseconds
        )
        return t,read+3

    def __read_date(self):
        time = self.read_uint24()
        if time == 0:  # nasty mysql 0000-00-00 dates
            return None

        year = (time & ((1 << 15) - 1) << 9) >> 9
        month = (time & ((1 << 4) - 1) << 5) >> 5
        day = (time & ((1 << 5) - 1))
        if year == 0 or month == 0 or day == 0:
            return None

        date = datetime.date(
            year=year,
            month=month,
            day=day
        )
        return date

    def __read_new_decimal(self, precision,decimals):
        """Read MySQL's new decimal format introduced in MySQL 5"""
        '''
        Each multiple of nine digits requires four bytes, and the “leftover” digits require some fraction of four bytes. 
        The storage required for excess digits is given by the following table. Leftover Digits	Number of Bytes
        
        Leftover Digits     Number of Bytes
        0	                0
        1	                1
        2	                1
        3	                2
        4	                2
        5	                3
        6	                3
        7	                4
        8	                4

        '''
        digits_per_integer = 9
        compressed_bytes = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4]
        integral = (precision - decimals)
        uncomp_integral = int(integral / digits_per_integer)
        uncomp_fractional = int(decimals / digits_per_integer)
        comp_integral = integral - (uncomp_integral * digits_per_integer)
        comp_fractional = decimals - (uncomp_fractional
                                             * digits_per_integer)

        _read_bytes = (uncomp_integral*4) + (uncomp_fractional*4) + compressed_bytes[comp_fractional] + compressed_bytes[comp_integral]

        _data = bytearray(self.read_bytes(_read_bytes))
        value = _data[0]
        if value & 0x80 != 0:
            res = ""
            mask = 0
        else:
            mask = -1
            res = "-"
        _data[0] = struct.pack('<B', value ^ 0x80)

        size = compressed_bytes[comp_integral]
        offset = 0
        if size > 0:
            offset += size
            value = self.read_int_be_by_size(size=size,bytes=_data) ^ mask
            res += str(value)

        for i in range(0, uncomp_integral):
            offset += 4
            value = struct.unpack('>i', _data[offset-4:offset])[0] ^ mask
            res += '%09d' % value

        res += "."

        for i in range(0, uncomp_fractional):
            offset += 4
            value = struct.unpack('>i', _data[offset-4:offset])[0] ^ mask
            res += '%09d' % value

        size = compressed_bytes[comp_fractional]
        if size > 0:
            value = self.read_int_be_by_size(size=size,bytes=_data[offset:]) ^ mask
            res += '%0*d' % (comp_fractional, value)

        return decimal.Decimal(res),_read_bytes

    def __is_null(self, null_bitmap, position):
        bit = null_bitmap[int(position / 8)]
        if type(bit) is str:
            bit = ord(bit)
        return bit & (1 << (position % 8))

    '''parsing for json'''
    '''################################################################'''
    def read_binary_json(self, length):
        t = self.read_uint8()
        return self.read_binary_json_type(t, length)

    def read_binary_json_type(self, t, length):
        large = (t in (json_type.JSONB_TYPE_LARGE_OBJECT, json_type.JSONB_TYPE_LARGE_ARRAY))
        if t in (json_type.JSONB_TYPE_SMALL_OBJECT, json_type.JSONB_TYPE_LARGE_OBJECT):
            return self.read_binary_json_object(length - 1, large)
        elif t in (json_type.JSONB_TYPE_SMALL_ARRAY, json_type.JSONB_TYPE_LARGE_ARRAY):
            return self.read_binary_json_array(length - 1, large)
        elif t in (json_type.JSONB_TYPE_STRING,):
            return self.read_length_coded_pascal_string(1)
        elif t in (json_type.JSONB_TYPE_LITERAL,):
            value = self.read_uint8()
            if value == json_type.JSONB_LITERAL_NULL:
                return None
            elif value == json_type.JSONB_LITERAL_TRUE:
                return True
            elif value == json_type.JSONB_LITERAL_FALSE:
                return False
        elif t == json_type.JSONB_TYPE_INT16:
            return self.read_int16()
        elif t == json_type.JSONB_TYPE_UINT16:
            return self.read_uint16()
        elif t in (json_type.JSONB_TYPE_DOUBLE,):
            return struct.unpack('<d', self.read(8))[0]
        elif t == json_type.JSONB_TYPE_INT32:
            return self.read_int32()
        elif t == json_type.JSONB_TYPE_UINT32:
            return self.read_uint32()
        elif t == json_type.JSONB_TYPE_INT64:
            return self.read_int64()
        elif t == json_type.JSONB_TYPE_UINT64:
            return self.read_uint64()

        raise ValueError('Json type %d is not handled' % t)

    def read_binary_json_type_inlined(self, t):
        if t == json_type.JSONB_TYPE_LITERAL:
            value = self.read_uint16()
            if value == json_type.JSONB_LITERAL_NULL:
                return None
            elif value == json_type.JSONB_LITERAL_TRUE:
                return True
            elif value == json_type.JSONB_LITERAL_FALSE:
                return False
        elif t == json_type.JSONB_TYPE_INT16:
            return self.read_int16()
        elif t == json_type.JSONB_TYPE_UINT16:
            return self.read_uint16()
        elif t == json_type.JSONB_TYPE_INT32:
            return self.read_int32()
        elif t == json_type.JSONB_TYPE_UINT32:
            return self.read_uint32()

        raise ValueError('Json type %d is not handled' % t)

    def read_binary_json_object(self, length, large):
        if large:
            elements = self.read_uint32()
            size = self.read_uint32()
        else:
            elements = self.read_uint16()
            size = self.read_uint16()

        if size > length:
            raise ValueError('Json length is larger than packet length')

        if large:
            key_offset_lengths = [(
                self.read_uint32(),  # offset (we don't actually need that)
                self.read_uint16()   # size of the key
                ) for _ in range(elements)]
        else:
            key_offset_lengths = [(
                self.read_uint16(),  # offset (we don't actually need that)
                self.read_uint16()   # size of key
                ) for _ in range(elements)]

        value_type_inlined_lengths = [self.read_offset_or_inline(large)
                                      for _ in range(elements)]

        keys = [self.__read_decode(x[1]) for x in key_offset_lengths]

        out = {}
        for i in range(elements):
            if value_type_inlined_lengths[i][1] is None:
                data = value_type_inlined_lengths[i][2]
            else:
                t = value_type_inlined_lengths[i][0]
                data = self.read_binary_json_type(t, length)
            out[keys[i]] = data

        return out

    def read_binary_json_array(self, length, large):
        if large:
            elements = self.read_uint32()
            size = self.read_uint32()
        else:
            elements = self.read_uint16()
            size = self.read_uint16()

        if size > length:
            raise ValueError('Json length is larger than packet length')

        values_type_offset_inline = [self.read_offset_or_inline(large) for _ in range(elements)]

        def _read(x):
            if x[1] is None:
                return x[2]
            return self.read_binary_json_type(x[0], length)

        return [_read(x) for x in values_type_offset_inline]

    def read_offset_or_inline(self,large):
        t = self.read_uint8()

        if t in (json_type.JSONB_TYPE_LITERAL,
                 json_type.JSONB_TYPE_INT16, json_type.JSONB_TYPE_UINT16):
            return (t, None, self.read_binary_json_type_inlined(t))
        if large and t in (json_type.JSONB_TYPE_INT32, json_type.JSONB_TYPE_UINT32):
            return (t, None, self.read_binary_json_type_inlined(t))

        if large:
            return (t, self.read_uint32(), None)
        return (t, self.read_uint16(), None)

    def read_length_coded_pascal_string(self, size):
        """Read a string with length coded using pascal style.
        The string start by the size of the string
        """
        length = self.read_uint_by_size(size)
        return self.__read_decode(length)
    '''###################################################'''

    def read_header(self):
        '''binlog_event_header_len = 19
        timestamp : 4bytes
        type_code : 1bytes
        server_id : 4bytes
        event_length : 4bytes
        next_position : 4bytes
        flags : 2bytes
        '''
        read_byte = self.read_bytes(19)
        if read_byte:
            result = struct.unpack('=IBIIIH',read_byte)
            _at_pos._at_pos = _at_pos._next_pos
            type_code,event_length,timestamp,_at_pos._next_pos = result[1],result[3],result[0],result[4]

            return type_code,event_length,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(timestamp))
        else:
            return None,None,None

    def read_query_event(self,event_length=None):
        '''fix_part = 13:
                thread_id : 4bytes
                execute_seconds : 4bytes
                database_length : 1bytes
                error_code : 2bytes
                variable_block_length : 2bytes
            variable_part :
                variable_block_length = fix_part.variable_block_length
                database_name = fix_part.database_length   
                sql_statement = event_header.event_length - 19 - 13 - variable_block_length - database_length - 4
        '''
        read_byte = self.read_bytes(binlog_event_fix_part)
        fix_result = struct.unpack('=IIBHH', read_byte)
        thread_id = fix_result[0]
        self.read_bytes(fix_result[4])
        read_byte = self.read_bytes(fix_result[2])
        database_name, = struct.unpack('{}s'.format(fix_result[2]), read_byte)
        statement_length = event_length - binlog_event_fix_part - binlog_event_header_len \
                           - fix_result[4] - fix_result[2] - binlog_quer_event_stern
        read_byte = self.read_bytes(statement_length)
        _a, sql_statement, = struct.unpack('1s{}s'.format(statement_length - 1), read_byte)
        return thread_id, database_name, sql_statement

    def read_table_map_event(self,event_length):
        '''
        fix_part = 8
            table_id : 6bytes
            Reserved : 2bytes
        variable_part:
            database_name_length : 1bytes
            database_name : database_name_length bytes + 1
            table_name_length : 1bytes
            table_name : table_name_length bytes + 1
            cloums_count : 1bytes
            colums_type_array : one byte per column  
            mmetadata_lenth : 1bytes
            metadata : .....(only available in the variable length field，varchar:2bytes，text、blob:1bytes,time、timestamp、datetime: 1bytes
                            blob、float、decimal : 1bytes, char、enum、binary、set: 2bytes(column type id :1bytes metadatea: 1bytes))
            bit_filed : 1bytes
            crc : 4bytes
            .........
        :param event_length: 
        :return: 
        '''
        self.read_bytes(table_map_event_fix_length)
        database_name_length, = struct.unpack('B',self.read_bytes(1))
        database_name,_a, = struct.unpack('{}ss'.format(database_name_length),self.read_bytes(database_name_length+1))
        table_name_length, = struct.unpack('B',self.read_bytes(1))
        table_name,_a, = struct.unpack('{}ss'.format(table_name_length),self.read_bytes(table_name_length+1))
        colums = self.read_uint8()
        a = '='
        for i in range(colums):
            a += 'B'
        colums_type_id_list = list(struct.unpack(a,self.read_bytes(colums)))
        self.read_bytes(1)
        metadata_dict = {}
        bytes = 1
        for idex in range(len(colums_type_id_list)):
            if colums_type_id_list[idex] in [column_type_dict.MYSQL_TYPE_VAR_STRING,column_type_dict.MYSQL_TYPE_VARCHAR]:
                metadata = self.read_uint16()
                metadata_dict[idex] = 2 if metadata > 255 else 1
                bytes += 2
            elif colums_type_id_list[idex] in [column_type_dict.MYSQL_TYPE_BLOB,column_type_dict.MYSQL_TYPE_MEDIUM_BLOB,column_type_dict.MYSQL_TYPE_LONG_BLOB,
                                               column_type_dict.MYSQL_TYPE_TINY_BLOB,column_type_dict.MYSQL_TYPE_JSON]:
                metadata = self.read_uint8()
                metadata_dict[idex] = metadata
                bytes += 1
            elif colums_type_id_list[idex] in [column_type_dict.MYSQL_TYPE_TIMESTAMP2 ,column_type_dict.MYSQL_TYPE_DATETIME2 ,column_type_dict.MYSQL_TYPE_TIME2]:
                metadata = self.read_uint8()
                metadata_dict[idex] = metadata
                bytes += 1
            elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_NEWDECIMAL:
                precision = self.read_uint8()
                decimals = self.read_uint8()
                metadata_dict[idex] = [precision,decimals]
                bytes += 2
            elif colums_type_id_list[idex] in  [column_type_dict.MYSQL_TYPE_FLOAT ,column_type_dict.MYSQL_TYPE_DOUBLE]:
                metadata = self.read_uint8()
                metadata_dict[idex] = metadata
                bytes += 1
            elif colums_type_id_list[idex] in [column_type_dict.MYSQL_TYPE_STRING]:
                _type,metadata, = struct.unpack('=BB',self.read_bytes(2))
                colums_type_id_list[idex] = _type
                metadata_dict[idex] = metadata
                bytes += 2

        if self.__packet is None:
            self.file_data.seek(event_length - binlog_event_header_len - table_map_event_fix_length - 5 - database_name_length
                       - table_name_length - colums  - bytes,1)
        return database_name,table_name,colums_type_id_list,metadata_dict

    def read_gtid_event(self,event_length=None):
        '''
        The layout of the buffer is as follows:
        +------+--------+-------+-------+--------------+---------------+
        |flags |SID     |GNO    |lt_type|last_committed|sequence_number|
        |1 byte|16 bytes|8 bytes|1 byte |8 bytes       |8 bytes        |
        +------+--------+-------+-------+--------------+---------------+
    
        The 'flags' field contains gtid flags.
            0 : rbr_only ,"/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*/%s\n"
            1 : sbr
    
        lt_type (for logical timestamp typecode) is always equal to the
        constant LOGICAL_TIMESTAMP_TYPECODE.
    
        5.6 did not have TS_TYPE and the following fields. 5.7.4 and
        earlier had a different value for TS_TYPE and a shorter length for
        the following fields. Both these cases are accepted and ignored.
    
        The buffer is advanced in Binary_log_event constructor to point to
        beginning of post-header
        :param event_length: 
        :return: 
        '''
        self.read_bytes(1)
        uuid = self.read_bytes(16)
        gtid = "%s%s%s%s-%s%s-%s%s-%s%s-%s%s%s%s%s%s" %\
               tuple("{0:02x}".format(ord(c)) for c in uuid)
        gno_id = self.read_uint64()
        gtid += ":{}".format(gno_id)
        if self.__packet is None:
            self.file_data.seek(event_length - 1 - 16 - 8 - binlog_event_header_len,1)
        return gtid

    def read_xid_variable(self):
        xid_num = self.read_uint64()
        return xid_num

    def __read_decode(self,count):
        _value = self.read_bytes(count)
        return struct.unpack('{}s'.format(count),_value)[0]

    def read_row_event(self,event_length,colums_type_id_list,metadata_dict,type,packet=None):
        '''
        fixed_part: 10bytes
            table_id: 6bytes
            reserved: 2bytes
            extra: 2bytes
        variable_part:
            columns: 1bytes
            variable_sized: int((n+7)/8) n=columns.value
            variable_sized: int((n+7)/8) (for updata_row_event only)
            
            variable_sized: int((n+7)/8)
            row_value : variable size
            
            crc : 4bytes
            
        The The data first length of the varchar type more than 255 are 2 bytes
        '''
        self.read_bytes(fix_length+binlog_row_event_extra_headers)
        columns = self.read_uint8()
        columns_length = (columns+7)/8
        self.read_bytes(columns_length)
        if type == binlog_events.UPDATE_ROWS_EVENT:
            self.read_bytes(columns_length)
            bytes = binlog_event_header_len + fix_length + binlog_row_event_extra_headers + 1 + columns_length + columns_length
        else:
            bytes = binlog_event_header_len + fix_length + binlog_row_event_extra_headers + 1 + columns_length
        __values = []
        while event_length - bytes > binlog_quer_event_stern:
            values = []
            null_bit = self.read_bytes(columns_length)
            bytes += columns_length
            for idex in range(len(colums_type_id_list)):
                if self.__is_null(null_bit, idex):
                    values.append('Null')
                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_TINY:
                    try:
                        values.append(self.read_uint8())
                    except:
                        values.append(self.read_int8())
                    bytes += 1
                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_SHORT:
                    try:
                        values.append(self.read_uint16())
                    except:
                        values.append(self.read_int16())
                    bytes += 2
                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_INT24:
                    try:
                        values.append(self.read_uint24())
                    except:
                        values.append(self.read_int24())
                    bytes += 3
                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_LONG:
                    try:
                        values.append(self.read_uint32())
                    except:
                        values.append(self.read_int32())
                    bytes += 4
                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_LONGLONG:
                    try:
                        values.append(self.read_uint64())
                    except:
                        values.append(self.read_int64())
                    bytes += 8

                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_NEWDECIMAL:
                    _list = metadata_dict[idex]
                    decimals,read_bytes = self.__read_new_decimal(precision=_list[0],decimals=_list[1])
                    values.append(decimals)
                    bytes += read_bytes

                elif colums_type_id_list[idex] in [column_type_dict.MYSQL_TYPE_DOUBLE ,column_type_dict.MYSQL_TYPE_FLOAT]:
                    _read_bytes = metadata_dict[idex]
                    if _read_bytes == 8:
                        _values, = struct.unpack('<d',self.read_bytes(_read_bytes))
                    elif _read_bytes == 4:
                        _values, = struct.unpack('<f', self.read_bytes(4))
                    values.append(_values)
                    bytes += _read_bytes

                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_TIMESTAMP2:
                    _time,read_bytes = self.__add_fsp_to_time(datetime.datetime.fromtimestamp(self.read_int_be_by_size(4)),metadata_dict[idex])
                    values.append(str(_time))
                    bytes += read_bytes + 4
                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_DATETIME2:
                    _time,read_bytes = self.__read_datetime2(metadata_dict[idex])
                    values.append(str(_time))
                    bytes += 5 + read_bytes
                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_YEAR:
                    _date = self.read_uint8() + 1900
                    values.append(_date)
                    bytes += 1
                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_DATE:
                    _time = self.__read_date()
                    values.append(str(_time))
                    bytes += 3

                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_TIME2:
                    _time,read_bytes = self.__read_time2(metadata_dict[idex])
                    bytes += read_bytes
                    values.append(str(_time))

                elif colums_type_id_list[idex] in [column_type_dict.MYSQL_TYPE_VARCHAR ,column_type_dict.MYSQL_TYPE_VAR_STRING ,column_type_dict.MYSQL_TYPE_BLOB,
                                                   column_type_dict.MYSQL_TYPE_TINY_BLOB,column_type_dict.MYSQL_TYPE_LONG_BLOB,column_type_dict.MYSQL_TYPE_MEDIUM_BLOB]:
                    _metadata = metadata_dict[idex]
                    value_length = self.read_uint_by_size(_metadata)
                    '''
                    if _metadata == 1:
                        value_length = self.read_uint8()
                    elif _metadata == 2:
                        value_length = self.read_uint16()
                    elif _metadata == 3:
                        value_length = self.read_uint24()
                    elif _metadata == 4:
                        value_length = self.read_uint32()
                    '''
                    values.append(str(self.__read_decode(value_length)))
                    bytes += value_length + _metadata
                elif colums_type_id_list[idex] in [column_type_dict.MYSQL_TYPE_JSON]:
                    _metadata = metadata_dict[idex]
                    value_length = self.read_uint_by_size(_metadata)
                    values.append(str(self.read_binary_json(value_length)))
                    bytes += value_length + _metadata
                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_STRING:
                    _metadata = metadata_dict[idex]
                    if _metadata <= 255:
                        value_length = self.read_uint8()
                        values.append(str(self.__read_decode(value_length)))
                        _read = 1
                    else:
                        value_length = self.read_uint16()
                        values.append(str(self.__read_decode(value_length)))
                        _read = 2
                    bytes += value_length + _read
                elif colums_type_id_list[idex] == column_type_dict.MYSQL_TYPE_ENUM:
                    _metadata = metadata_dict[idex]
                    if _metadata == 1:
                        values.append('@'+str(self.read_uint8()))
                    elif _metadata == 2:
                        values.append('@'+str(self.read_uint16()))
                    bytes += _metadata

            if type == binlog_events.UPDATE_ROWS_EVENT:
                __values.append(values)
            else:
                super(Read,self).TractionVlues(after_value=values,type=type)
        if self.__packet is None:
            self.file_data.seek(event_length - bytes,1)
        return __values



    def write_row_event(self,event_length,colums_type_id_list,metadata_dict,type):
        self.read_row_event(event_length,colums_type_id_list,metadata_dict,type)

    def delete_row_event(self,event_length,colums_type_id_list,metadata_dict,type):
        self.read_row_event(event_length, colums_type_id_list, metadata_dict,type)

    def update_row_event(self,event_length,colums_type_id_list,metadata_dict,type):
        values = self.read_row_event(event_length,colums_type_id_list,metadata_dict,type)
        __values = [values[i:i+2] for i in xrange(0,len(values),2)]
        for i in range(len(__values)):
            super(Read,self).TractionVlues(before_value=__values[i][0],after_value=__values[i][1],type=type)



class CheckEvent(Echo):
    def __init__(self,filename=None,start_position=None,stop_position=None,
                 start_datetime=None,stop_datetime=None,_thread_id=None,gtid=None,rollback=None,
                 user=None,host=None,passwd=None,port=None):
        self.cloums_type_id_list = None
        self.metadata_dict = None
        self._gtid = None
        self._thread_id_status = None
        self._func = None

        self.start_position = start_position
        self.stop_position = stop_position
        self.start_datetime = start_datetime
        self.stop_datetime = stop_datetime
        self._thread_id = _thread_id
        self.gtid = gtid
        if rollback:
            if user is None or host is None or passwd is None:
                Usage()
                sys.exit()
            _rollback.rollback_status = True
            _rollback._myfunc = GetRollStatement(host=host,user=user,passwd=passwd,port=(port if port else 3306))
            if os.path.exists('tmp_rollback'):
                os.remove('tmp_rollback')
            _rollback._myfile = open('tmp_rollback','a+')

        _at_pos._next_pos = start_position if start_position else  4
        if filename is None:
            print 'NO SUCH FILE '
            Usage()
            sys.exit()

        self.readbinlog = Read(start_position=start_position,filename=filename)
        self.__read()

    def __gtid_event_filter(self,type,type_code=None,event_length=None,execute_time=None):
        if type_code is None and event_length is None and execute_time is None:
            type_code, event_length, execute_time = self.readbinlog.read_header()
        while True:
            if type == 'pos' and _at_pos._at_pos > self.stop_position and self.stop_position \
                    and type_code == binlog_events.GTID_LOG_EVENT:
                break
            elif type == 'datetime' and self.stop_datetime and execute_time > self.stop_datetime:
                break
            if type_code is None:
                break
            if self._gtid is None:
                if type_code == binlog_events.GTID_LOG_EVENT:
                    gtid = self.readbinlog.read_gtid_event(event_length)
                    if gtid == self.gtid:
                        self._gtid = gtid
                        self.Gtid(execute_time, gtid, _at_pos._at_pos)
                else:
                    self.readbinlog.file_data.seek(event_length-binlog_event_header_len,1)
            else:
                if type_code == binlog_events.GTID_LOG_EVENT:
                    break
                self.__read_event(type_code,event_length,execute_time)
            type_code, event_length, execute_time = self.readbinlog.read_header()

    def __thread_id_filed(self,type,type_code=None,event_length=None,execute_time=None):
        if type_code is None and event_length is None and execute_time is None:
            type_code, event_length, execute_time = self.readbinlog.read_header()
        while True:
            if type == 'pos' and _at_pos._at_pos > self.stop_position and self.stop_position \
                    and type_code == binlog_events.GTID_LOG_EVENT:
                break
            elif type == 'datetime' and self.stop_datetime and execute_time > self.stop_datetime:
                break
            if type_code is None:
                break

            if type_code == binlog_events.QUERY_EVENT and self._thread_id_status is None:
                thread_id, database_name, sql_statement = self.readbinlog.read_query_event(event_length)
                if thread_id == self._thread_id:
                    if self._gtid:
                        self.Gtid(execute_time, self._gtid, _at_pos._at_pos)
                    self._thread_id_status = True
                    self.TractionHeader(thread_id, database_name, sql_statement, execute_time, _at_pos._at_pos)
                self.readbinlog.file_data.seek(4,1)
            elif self._thread_id_status and type_code != binlog_events.QUERY_EVENT and type_code != binlog_events.GTID_LOG_EVENT:
                self.__read_event(type_code,event_length,execute_time)
            elif type_code == binlog_events.QUERY_EVENT and self._thread_id_status:
                thread_id, database_name, sql_statement = self.readbinlog.read_query_event(event_length)
                if thread_id != self._thread_id:
                    self._thread_id_status = None
                else:
                    self.TractionHeader(thread_id, database_name, sql_statement, execute_time, _at_pos._at_pos)
                self.readbinlog.file_data.seek(4, 1)
            elif type_code == binlog_events.GTID_LOG_EVENT and self._thread_id_status is None:
                self._gtid = self.readbinlog.read_gtid_event(event_length)
            else:
                self.readbinlog.file_data.seek(event_length-binlog_event_header_len,1)
            type_code, event_length, execute_time = self.readbinlog.read_header()

    def __read_event(self,type_code,event_length,execute_time):
        if type_code == binlog_events.FORMAT_DESCRIPTION_EVENT:
            binlog_ver, server_ver, create_time = self.readbinlog.read_format_desc_event()
            self.Version(binlog_ver, server_ver, create_time)
            self.readbinlog.file_data.seek(event_length - binlog_event_header_len - read_format_desc_event_length,1)
        elif type_code == binlog_events.QUERY_EVENT:
            thread_id, database_name, sql_statement = self.readbinlog.read_query_event(event_length)
            self.TractionHeader(thread_id, database_name, sql_statement, execute_time, _at_pos._at_pos)
            self.readbinlog.file_data.seek(4,1)
        elif type_code == binlog_events.XID_EVENT:
            xid_num = self.readbinlog.read_xid_variable()
            self.Xid(execute_time, xid_num, _at_pos._at_pos)
            self.readbinlog.file_data.seek(4,1)
        elif type_code == binlog_events.TABLE_MAP_EVENT:
            database_name, table_name, self.cloums_type_id_list, self.metadata_dict = self.readbinlog.read_table_map_event(
                event_length)
            self.Tablemap(execute_time, table_name)
        elif type_code == binlog_events.GTID_LOG_EVENT:
            gtid = self.readbinlog.read_gtid_event(event_length)
            self.Gtid(execute_time, gtid, _at_pos._at_pos)
        elif type_code == binlog_events.WRITE_ROWS_EVENT:
            self.readbinlog.write_row_event(event_length, self.cloums_type_id_list, self.metadata_dict, type_code)
        elif type_code == binlog_events.DELETE_ROWS_EVENT:
            self.readbinlog.delete_row_event(event_length, self.cloums_type_id_list, self.metadata_dict, type_code)
        elif type_code == binlog_events.UPDATE_ROWS_EVENT:
            self.readbinlog.update_row_event(event_length, self.cloums_type_id_list, self.metadata_dict, type_code)
        else:
            self.readbinlog.file_data.seek(event_length - binlog_event_header_len,1)

    def __read_binlog(self,type,type_code=None,event_length=None,execute_time=None):
        if type_code is None and event_length is None and execute_time is None:
            type_code, event_length, execute_time = self.readbinlog.read_header()
        while True:
            if type == 'pos' and _at_pos._at_pos > self.stop_position and self.stop_position and \
                            type_code == binlog_events.GTID_LOG_EVENT:
                break
            elif type == 'datetime' and self.stop_datetime and execute_time > self.stop_datetime:
                break
            if type_code is None:
                break
            self.__read_event(type_code=type_code,event_length=event_length,execute_time=execute_time)
            type_code, event_length, execute_time = self.readbinlog.read_header()

    def __read(self):
        if self.start_position:
            if self.gtid:
                self.__gtid_event_filter('pos')
            elif self._thread_id:
                self.__thread_id_filed('pos')
            else:
                self.__read_binlog('pos')
        elif self.start_datetime:
            while True:
                type_code, event_length, execute_time = self.readbinlog.read_header()
                if  execute_time >= self.start_datetime:
                    break
                self.readbinlog.file_data.seek(event_length - binlog_event_header_len,1)
            if self.gtid:
                self.__gtid_event_filter('datetime',type_code,event_length,execute_time)
            elif self._thread_id:
                self.__thread_id_filed('datetime',type_code,event_length,execute_time)
            else:
                self.__read_binlog('datetime',type_code,event_length,execute_time)
        else:
            if self.gtid:
                self.__gtid_event_filter('pos')
            elif self._thread_id:
                self.__thread_id_filed('pos')
            else:
                self.__read_binlog('pos')

        self.readbinlog.file_data.close()
        if _rollback.rollback_status:
            _rollback._myfunc._close()
            ps = PrintSql()
            ps.read()

class ReplicationMysql(Echo):
    def __init__(self,block = None,server_id = None,log_file = None,
                 log_pos = None,host=None,user=None,passwd=None,rollback=None,
                 port = None,gtid = None,_thread_id = None,stop_pos=None):
        import pymysql
        _remote_filed._gtid = gtid
        _remote_filed._thread_id = _thread_id

        self._stop_pos = stop_pos
        self._log_file = log_file
        self._log_pos = log_pos
        self.block = block if block != None else False
        self.server_id = server_id if server_id != None else 133
        self.port = port if port != None else 3306
        self.connection = pymysql.connect(host=host,
                                     user=user,
                                     password=passwd,port=self.port,
                                     db='',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        if rollback:
            _remote_filed._rollback_status = True
            _rollback._myfunc = GetRollStatement(host=host,user=user,passwd=passwd,port=self.port)

        self.ReadPack()

    def __checksum_enabled(self):
        """Return True if binlog-checksum = CRC32. Only for MySQL > 5.6"""
        with self.connection.cursor() as cur:
            sql = 'SHOW GLOBAL VARIABLES LIKE "BINLOG_CHECKSUM";'
            cur.execute(sql)
            result = cur.fetchone()

        if result is None:
            return False
        if 'Value' in result and result['Value'] is None:
            return False
        return True

    def __set_checksum(self):
        with self.connection.cursor() as cur:
            cur.execute("set @master_binlog_checksum= @@global.binlog_checksum;")

    def GetFile(self):
        with self.connection.cursor() as cur:
            sql = "show master status;"
            cur.execute(sql)
            result = cur.fetchone()
            return result['File'],result['Position']

    def PackeByte(self):
        '''
        Format for mysql packet position
        file_length: 4bytes
        dump_type: 1bytes
        position: 4bytes
        flags: 2bytes  
            0: BINLOG_DUMP_BLOCK
            1: BINLOG_DUMP_NON_BLOCK
        server_id: 4bytes
        log_file
        :return: 
        '''
        COM_BINLOG_DUMP = 0x12

        if self._log_file is None:
            if self._log_pos is None:
                self._log_file,self._log_pos = self.GetFile()
            else:
                self._log_file,_ = self.GetFile()
        elif self._log_file and self._log_pos is None:
            self._log_pos = 4

        prelude = struct.pack('<i', len(self._log_file) + 11) \
                  + struct.pack("!B", COM_BINLOG_DUMP)

        prelude += struct.pack('<I', self._log_pos)
        if self.block:
            prelude += struct.pack('<h', 0)
        else:
            prelude += struct.pack('<h', 1)

        prelude += struct.pack('<I', self.server_id)
        prelude += self._log_file.encode()
        return prelude


    def UnPack(self,pack):
        #header
        next_log_pos = None
        unpack = struct.unpack('<cIcIIIH', pack.read(20))
        _Read = Read(pack=pack)
        if unpack[1] != 0:
            execute_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(unpack[1]))
            if isinstance(unpack[2], int):
                event_type = unpack[2]
            else:
                event_type = struct.unpack("!B", unpack[2])[0]
            server_id,event_length,next_log_pos = unpack[3],unpack[4],unpack[5]
            if event_type == binlog_events.QUERY_EVENT:
                thread_id, database_name, sql_statement = _Read.read_query_event(event_length)
                self.TractionHeader(thread_id, database_name, sql_statement, execute_time, self._log_pos)

            elif event_type == binlog_events.GTID_LOG_EVENT:
                gtid = _Read.read_gtid_event()
                self.Gtid(execute_time,gtid,self._log_pos)

            elif event_type == binlog_events.XID_EVENT:
                xid = _Read.read_xid_variable()
                self.Xid(execute_time,xid,self._log_pos)
            elif event_type == binlog_events.TABLE_MAP_EVENT:
                database_name, table_name, self.cloums_type_id_list, self.metadata_dict = _Read.read_table_map_event(event_length)
                self.Tablemap(execute_time, table_name)
            elif event_type == binlog_events.WRITE_ROWS_EVENT:
                _Read.write_row_event(event_length, self.cloums_type_id_list, self.metadata_dict, event_type)
            elif event_type == binlog_events.DELETE_ROWS_EVENT:
                _Read.delete_row_event(event_length, self.cloums_type_id_list, self.metadata_dict, event_type)
            elif event_type == binlog_events.UPDATE_ROWS_EVENT:
                _Read.update_row_event(event_length, self.cloums_type_id_list, self.metadata_dict, event_type)
        if next_log_pos:
            self._log_pos = next_log_pos

    def ReadPack(self):
        _packet = self.PackeByte()
        if self.__checksum_enabled():
            self.__set_checksum()
        import pymysql
        if pymysql.__version__ < "0.6":
            self.connection.wfile.write(_packet)
            self.connection.wfile.flush()
        else:
            self.connection._write_bytes(_packet)
            self.connection._next_seq_id = 1

        while True:
            try:
                if pymysql.__version__ < "0.6":
                    pkt = self.connection.read_packet()
                else:
                    pkt = self.connection._read_packet()

                self.UnPack(pkt)
            except:
                self.connection.close()
                break
            if self._stop_pos and self._log_pos > self._stop_pos:
                break


def Usage():
    __usage__ = """
    	Usage:
    	Options:
      		-h [--help] : print help message
      		-f [--file] : the file path
      		--start-position : Start reading the binlog at position N. Applies to the
                                    first binlog passed on the command line.
    		--stop-position :   Stop reading the binlog at position N. Applies to the
                                last binlog passed on the command line.
            --start-datetime :   Start reading the binlog at first event having a datetime
                                  equal or posterior to the argument; the argument must be
                                  a date and time in the local time zone, in any format
                                  accepted by the MySQL server for DATETIME and TIMESTAMP
                                  types, for example: 2004-12-25 11:25:56 (you should
                                  probably use quotes for your shell to set it properly)
            --stop-datetime :    Stop reading the binlog at first event having a datetime
                                  equal or posterior to the argument; the argument must be
                                  a date and time in the local time zone, in any format
                                  accepted by the MySQL server for DATETIME and TIMESTAMP
                                  types, for example: 2004-12-25 11:25:56 (you should
                                  probably use quotes for your shell to set it properly).
            -t [--thread-id] :    filter the executing thread id
            
            -g [--gtid] : obtain a certain GTID content of transactions  
            
            -r [--rollback] : generate the rollback statement 
                -u [--user] : User for login if not current user
                -p [--passwd] : Password to use when connecting to server
                -H [--host] : Connect to host, default localhost
                -P [--port] : Port number to use for connection ,default 3306
            --remote:  Replication slave
                --log-file: Set replication log file, default master current log file
                --start-position: Start replication at log position (resume_stream should be true),default current position
                --stop-position: Stop replication at log position
                --block: Read on stream is blocking
                --server-id:  default 133
    	    """
    print __usage__

def main(argv):
    _argv = {}
    try:
        opts, args = getopt.getopt(argv[1:], 'hrf:t:g:H:p:P:u:', ['help', 'file=', 'start-position=','stop-position=','start-datetime=',
                                                          'stop-datetime=','host=','user=','passwd=','port=','thread-id=','gtid=',
                                                          'rollback','remote','log-file=','block','server-id='])
    except getopt.GetoptError, err:
        print str(err)
        Usage()
        sys.exit(2)
    for o, a in opts:
        if o in ('-h', '--help'):
            Usage()
            sys.exit(1)
        elif o in ('-f', '--file'):
            _argv['file'] = a
        elif o in ('--start-position',):
            _argv['start-position'] = int(a)
        elif o in ('--stop-position',):
            _argv['stop-position'] = int(a)
        elif o in ('--start-datetime'):
            _argv['start-datetime'] = a
        elif o in ('--stop-datetime'):
            _argv['stop-datetime'] = a
        elif o in ('-t','--thread-id'):
            _argv['thread-id'] = int(a)
        elif o in ('-g','--gtid'):
            _argv['gtid'] = a
        elif o in ('-r','--rollback'):
            _argv['rollback'] = True
        elif o in ('-u','--user'):
            _argv['user'] = a
        elif o in ('-H','--host'):
            _argv['host'] = a
        elif o in ('-p','--passwd'):
            _argv['passwd'] = a
        elif o in ('-P','--port'):
            _argv['port'] = int(a)
        elif o in ('--remote'):
            _argv['remote'] = True
        elif o in ('--log-file'):
            _argv['log-file'] = a
        elif o in ('--block'):
            _argv['block'] = True
        elif o in ('--server-id'):
            _argv['server-id'] = int(a)
        else:
            print 'unhandled option'
            sys.exit(3)

    if 'rollback' in _argv:
        if 'remote' in _argv:
            ReplicationMysql(user=_argv['user'],port=(_argv['port'] if 'port' in _argv else None),
                             passwd=_argv['passwd'],host=(_argv['host'] if 'host' in _argv else 'localhost'),
                             log_file=(_argv['log-file'] if 'log-file' in _argv else None),
                             log_pos=(_argv['start-position'] if 'start-position' in _argv else None),
                             stop_pos=(_argv['stop-position'] if 'stop-position' in _argv else None),
                             server_id=(_argv['server-id'] if 'server-id' in _argv else None),
                             block=(_argv['block'] if 'block' in _argv else None),
                             gtid=(_argv['gtid'] if 'gtid' in _argv else None),
                             _thread_id=(_argv['thread-id'] if 'thread-id' in _argv else None),
                             rollback=(_argv['rollback'] if 'rollback' in _argv else None))
        elif 'start-position' in _argv:
            CheckEvent(filename=_argv['file'],gtid=(_argv['gtid'] if 'gtid' in _argv else None),
                       start_position=(_argv['start-position'] if 'start-position' in _argv else None),
                       stop_position=(_argv['stop-position'] if 'stop-position' in _argv else None),
                       rollback = _argv['rollback'],user=_argv['user'],
                       host=(_argv['host'] if 'host' in _argv else 'localhost'),
                       passwd=_argv['passwd'],
                       port=(_argv['port'] if 'port' in _argv else None),
                       _thread_id=(_argv['thread-id'] if 'thread-id' in _argv else None))
        elif 'gtid' in _argv:
            CheckEvent(filename=_argv['file'],
                       gtid=(_argv['gtid'] if 'gtid' in _argv else None),rollback=_argv['rollback'],
                       user=_argv['user'],
                       host=(_argv['host'] if 'host' in _argv else 'localhost'),
                       passwd=_argv['passwd'],
                       port=(_argv['port'] if 'port' in _argv else None))
        else:
            CheckEvent(filename=_argv['file'],rollback=_argv['rollback'],user=_argv['user'],
                       host=(_argv['host'] if 'host' in _argv else 'localhost'),
                       passwd=_argv['passwd'],
                       port=(_argv['port'] if 'port' in _argv else None),
                       _thread_id=(_argv['thread-id'] if 'thread-id' in _argv else None))
    elif 'remote' in _argv:
        ReplicationMysql(user=_argv['user'], port=(_argv['port'] if 'port' in _argv else None),
                         passwd=_argv['passwd'], host=(_argv['host'] if 'host' in _argv else 'localhost'),
                         log_file=(_argv['log-file'] if 'log-file' in _argv else None),
                         log_pos=(_argv['start-position'] if 'start-position' in _argv else None),
                         stop_pos=(_argv['stop-position'] if 'stop-position' in _argv else None),
                         server_id=(_argv['server-id'] if 'server-id' in _argv else None),
                         block=(_argv['block'] if 'block' in _argv else None),
                         gtid=(_argv['gtid'] if 'gtid' in _argv else None),
                         _thread_id=(_argv['thread-id'] if 'thread-id' in _argv else None),
                         rollback=(_argv['rollback'] if 'rollback' in _argv else None))

    elif 'start-position' in _argv:
        CheckEvent(start_position=(_argv['start-position'] if _argv['start-position'] else None),
                        filename=_argv['file'],gtid=(_argv['gtid'] if 'gtid' in _argv else None),
                        stop_position=(_argv['stop-position'] if 'stop-position' in _argv else None),
                        _thread_id=(_argv['thread-id'] if 'thread-id' in _argv else None))
    elif 'start-datetime' in _argv:
        CheckEvent(start_datetime=(_argv['start-datetime'] if 'start-datetime' in _argv else None),
                        filename=_argv['file'],gtid=(_argv['gtid'] if 'gtid' in _argv else None),
                        stop_datetime=(_argv['stop-datetime'] if 'stop-datetime' in _argv else None),
                        _thread_id=(_argv['thread-id'] if 'thread-id' in _argv else None))
    elif 'gtid' in _argv:
        CheckEvent(filename=_argv['file'],
                        gtid=(_argv['gtid'] if 'gtid' in _argv else None),
                        _thread_id=(_argv['thread-id'] if 'thread-id' in _argv else None))
    else:
        CheckEvent(filename=_argv['file'],
                        _thread_id=(_argv['thread-id'] if 'thread-id' in _argv else None))



if __name__ == "__main__":
    main(sys.argv)



