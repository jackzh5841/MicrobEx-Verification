PROCESSOR_NUM = 12
MULTI_PROCESS = True
OUTPUT_PARSED_TEXT = False
#try to use lower casse if possible for string content


SQL_MODE = False
DB_DUMP = 'ucxdatadump'
DB_DEV = 'ucxdev'
DB_FOLDER = '/home/jzhang/project/db/'

CSV_RAW_FILE_PATH= DB_FOLDER + 'QFHT_CS_Raw_lab_data.csv'
CSV_CNS_FILE_PATH= DB_FOLDER + 'QFHT_CNS.csv'
CSV_CNS_LABID_FILE_PATH = DB_FOLDER + 'cns_with_lab_id.csv'
HUMAN_VALIDATED_RES_FILE_PATH = DB_FOLDER + 'res_human_validated.csv'

CSV_RES_FILE_PATH = ''

if not SQL_MODE:
    CSV_RES_FILE_PATH = DB_FOLDER + 'res_2018.csv'

TB_HL7MSG = 'hl7textmessage'
TB_HL7INFO = 'hl7textinfo'
TB_CSV_RAW = 'csvRaw'
TB_INBOX = 'ucxhl7_inbox'

# (lab_id, message), 0 based
array_column_number_hl7 = (0, 2)
array_column_number_csv = (1, 11) 
# TB_UCX_HL7 = 'hl7textmessage1697348881436150'

HL7_UCX_SEG = 'zmc'
HL7_UCX_DEC = 'urine culture'
HL7_UCX_ID = 'curn'
HL7_UCX_MIX_ID = 'urmx'
HL7_UCX_LENGTH = 50
arr_hl7_seg_length = []

import re 
HL7_UCX_ID_REG = fr'({HL7_UCX_SEG}\|.*\|{HL7_UCX_ID})'
HL7_CULTURE_REG = fr'({HL7_UCX_SEG}\|.*\|culture)'
HL7_UCX_DESC_REG = fr'({HL7_UCX_SEG}(\|.*\|)+{HL7_UCX_DEC})'

IDX_POS_CUL_STATUS_IN_CSV = 5

import time
import re
STR_TIME = str(int(time.time_ns() / 1000))

import mysql.connector
class MysqlDb: #https://stackoverflow.com/questions/38076220/python-mysqldb-connection-in-a-class
    def __init__(self, db_name):
        self._cnx = mysql.connector.connect(user = 'jzhang', password = 'NewSql000)', host = 'localhost', database = db_name)
        self._curs = self._cnx.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def connection(self):
        return self._cnx

    @property
    def cursor(self):
        return self._curs

    def commit(self):
        self.connection.commit()

    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()
    

list_abx_formal = ['amoxicillin',
 'amoxil',
 'trimox',
 'azithromycin',
 'zithromax',
 'azithrocin',
 'cephalexin',
 'keflex',
 'ceporex',
 'ciprofloxacin',
 'proquin',
 'clindamycin',
 'cleocin',
 'dalacin',
 'clinda'
 'doxycycline',
 'vibramycin',
 'doryx',
 'erythromycin',
 'eryc',
 'erythrocin',
 'levofloxacin',
 'levaquin',
 'tavanic',
 'metronidazole',
 'flagyl',
 'metrogel',
 'penicillin',
 'penvee',
 'veetids',
 'tetracycline',
 'sumycin',
 'achromycin',
 'trimethoprim/sulfamethoxazole',
 'bactrim',
 'septra',
 'vancomycin',
 'vancocin',
 'vancoled',
 'nitrofurantoin',
 'macrobid',
 'macrodantin',
 'fosfomycin',
 'monurol',
 'amoxicillinclavulanate',
 'augmentin',
 'clavulin']

list_abx_informal = ['amox', 'amoxi', 'clinda', 'doxy', 'cipro', 'fosfo',  'amox-clav']

list_abx = list_abx_formal + list_abx_informal

list_on_rx = ['on', 'prescribed', 'taking', 'treated', 'treatment', 'rx']


re_abx_pattern = fr"(?<!no)\b(?:{'|'.join(list_on_rx)})\W+(?:{'|'.join(list_abx)})\b"




