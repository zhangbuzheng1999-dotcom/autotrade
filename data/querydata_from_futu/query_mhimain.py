import queue
import numpy as np
from futu import *
import pandas as pd
from sqlalchemy import create_engine
import threading
import datetime
import sys
import warnings
import traceback
from conn import klinepubsub
from coreutils.logger import get_logger
from coreutils.config import DatabaseInfo
from coreutils.constant import Interval
warnings.filterwarnings('ignore')

sys.path.append('//')
# 获取数据库基本信息
# cursor用于执行cursor.execute（）

# conn_to_sql用于写入数据库
conn_to_sql = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format(
        DatabaseInfo.user, DatabaseInfo.password, DatabaseInfo.host, DatabaseInfo.port, "HKEX"
    ),
    pool_size=15, pool_recycle=1600, pool_pre_ping=True, pool_use_lifo=True, echo_pool=True, max_overflow=5)

code = 'HK.MHImain'
quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
# 发布k线程序
publish_kline = klinepubsub.PubKline(ip='127.0.0.1', port=20250)

# 日记记录
logger = get_logger(__name__)  # 写入 logs/run.log（每天切分）

start_time_5m = ['00:00:00', '00:05:00', '00:10:00', '00:15:00', '00:20:00', '00:25:00', '00:30:00', '00:35:00',
                 '00:40:00', '00:45:00', '00:50:00', '00:55:00', '01:00:00', '01:05:00', '01:10:00', '01:15:00',
                 '01:20:00', '01:25:00', '01:30:00', '01:35:00', '01:40:00', '01:45:00', '01:50:00', '01:55:00',
                 '02:00:00', '02:05:00', '02:10:00', '02:15:00', '02:20:00', '02:25:00', '02:30:00', '02:35:00',
                 '02:40:00', '02:45:00', '02:50:00', '02:55:00', '03:00:00', '03:05:00', '03:10:00', '03:15:00',
                 '03:20:00', '03:25:00', '03:30:00', '03:35:00', '03:40:00', '03:45:00', '03:50:00', '03:55:00',
                 '04:00:00', '04:05:00', '04:10:00', '04:15:00', '04:20:00', '04:25:00', '04:30:00', '04:35:00',
                 '04:40:00', '04:45:00', '04:50:00', '04:55:00', '05:00:00', '05:05:00', '05:10:00', '05:15:00',
                 '05:20:00', '05:25:00', '05:30:00', '05:35:00', '05:40:00', '05:45:00', '05:50:00', '05:55:00',
                 '06:00:00', '06:05:00', '06:10:00', '06:15:00', '06:20:00', '06:25:00', '06:30:00', '06:35:00',
                 '06:40:00', '06:45:00', '06:50:00', '06:55:00', '07:00:00', '07:05:00', '07:10:00', '07:15:00',
                 '07:20:00', '07:25:00', '07:30:00', '07:35:00', '07:40:00', '07:45:00', '07:50:00', '07:55:00',
                 '08:00:00', '08:05:00', '08:10:00', '08:15:00', '08:20:00', '08:25:00', '08:30:00', '08:35:00',
                 '08:40:00', '08:45:00', '08:50:00', '08:55:00', '09:00:00', '09:05:00', '09:10:00', '09:15:00',
                 '09:20:00', '09:25:00', '09:30:00', '09:35:00', '09:40:00', '09:45:00', '09:50:00', '09:55:00',
                 '10:00:00', '10:05:00', '10:10:00', '10:15:00', '10:20:00', '10:25:00', '10:30:00', '10:35:00',
                 '10:40:00', '10:45:00', '10:50:00', '10:55:00', '11:00:00', '11:05:00', '11:10:00', '11:15:00',
                 '11:20:00', '11:25:00', '11:30:00', '11:35:00', '11:40:00', '11:45:00', '11:50:00', '11:55:00',
                 '12:00:00', '12:05:00', '12:10:00', '12:15:00', '12:20:00', '12:25:00', '12:30:00', '12:35:00',
                 '12:40:00', '12:45:00', '12:50:00', '12:55:00', '13:00:00', '13:05:00', '13:10:00', '13:15:00',
                 '13:20:00', '13:25:00', '13:30:00', '13:35:00', '13:40:00', '13:45:00', '13:50:00', '13:55:00',
                 '14:00:00', '14:05:00', '14:10:00', '14:15:00', '14:20:00', '14:25:00', '14:30:00', '14:35:00',
                 '14:40:00', '14:45:00', '14:50:00', '14:55:00', '15:00:00', '15:05:00', '15:10:00', '15:15:00',
                 '15:20:00', '15:25:00', '15:30:00', '15:35:00', '15:40:00', '15:45:00', '15:50:00', '15:55:00',
                 '16:00:00', '16:05:00', '16:10:00', '16:15:00', '16:20:00', '16:25:00', '16:30:00', '16:35:00',
                 '16:40:00', '16:45:00', '16:50:00', '16:55:00', '17:00:00', '17:05:00', '17:10:00', '17:15:00',
                 '17:20:00', '17:25:00', '17:30:00', '17:35:00', '17:40:00', '17:45:00', '17:50:00', '17:55:00',
                 '18:00:00', '18:05:00', '18:10:00', '18:15:00', '18:20:00', '18:25:00', '18:30:00', '18:35:00',
                 '18:40:00', '18:45:00', '18:50:00', '18:55:00', '19:00:00', '19:05:00', '19:10:00', '19:15:00',
                 '19:20:00', '19:25:00', '19:30:00', '19:35:00', '19:40:00', '19:45:00', '19:50:00', '19:55:00',
                 '20:00:00', '20:05:00', '20:10:00', '20:15:00', '20:20:00', '20:25:00', '20:30:00', '20:35:00',
                 '20:40:00', '20:45:00', '20:50:00', '20:55:00', '21:00:00', '21:05:00', '21:10:00', '21:15:00',
                 '21:20:00', '21:25:00', '21:30:00', '21:35:00', '21:40:00', '21:45:00', '21:50:00', '21:55:00',
                 '22:00:00', '22:05:00', '22:10:00', '22:15:00', '22:20:00', '22:25:00', '22:30:00', '22:35:00',
                 '22:40:00', '22:45:00', '22:50:00', '22:55:00', '23:00:00', '23:05:00', '23:10:00', '23:15:00',
                 '23:20:00', '23:25:00', '23:30:00', '23:35:00', '23:40:00', '23:45:00', '23:50:00', '23:55:00']
end_time_5m = ['00:04:59', '00:09:59', '00:14:59', '00:19:59', '00:24:59', '00:29:59', '00:34:59', '00:39:59',
               '00:44:59', '00:49:59', '00:54:59', '00:59:59', '01:04:59', '01:09:59', '01:14:59', '01:19:59',
               '01:24:59', '01:29:59', '01:34:59', '01:39:59', '01:44:59', '01:49:59', '01:54:59', '01:59:59',
               '02:04:59', '02:09:59', '02:14:59', '02:19:59', '02:24:59', '02:29:59', '02:34:59', '02:39:59',
               '02:44:59', '02:49:59', '02:54:59', '02:59:59', '03:04:59', '03:09:59', '03:14:59', '03:19:59',
               '03:24:59', '03:29:59', '03:34:59', '03:39:59', '03:44:59', '03:49:59', '03:54:59', '03:59:59',
               '04:04:59', '04:09:59', '04:14:59', '04:19:59', '04:24:59', '04:29:59', '04:34:59', '04:39:59',
               '04:44:59', '04:49:59', '04:54:59', '04:59:59', '05:04:59', '05:09:59', '05:14:59', '05:19:59',
               '05:24:59', '05:29:59', '05:34:59', '05:39:59', '05:44:59', '05:49:59', '05:54:59', '05:59:59',
               '06:04:59', '06:09:59', '06:14:59', '06:19:59', '06:24:59', '06:29:59', '06:34:59', '06:39:59',
               '06:44:59', '06:49:59', '06:54:59', '06:59:59', '07:04:59', '07:09:59', '07:14:59', '07:19:59',
               '07:24:59', '07:29:59', '07:34:59', '07:39:59', '07:44:59', '07:49:59', '07:54:59', '07:59:59',
               '08:04:59', '08:09:59', '08:14:59', '08:19:59', '08:24:59', '08:29:59', '08:34:59', '08:39:59',
               '08:44:59', '08:49:59', '08:54:59', '08:59:59', '09:04:59', '09:09:59', '09:14:59', '09:19:59',
               '09:24:59', '09:29:59', '09:34:59', '09:39:59', '09:44:59', '09:49:59', '09:54:59', '09:59:59',
               '10:04:59', '10:09:59', '10:14:59', '10:19:59', '10:24:59', '10:29:59', '10:34:59', '10:39:59',
               '10:44:59', '10:49:59', '10:54:59', '10:59:59', '11:04:59', '11:09:59', '11:14:59', '11:19:59',
               '11:24:59', '11:29:59', '11:34:59', '11:39:59', '11:44:59', '11:49:59', '11:54:59', '11:59:59',
               '12:04:59', '12:09:59', '12:14:59', '12:19:59', '12:24:59', '12:29:59', '12:34:59', '12:39:59',
               '12:44:59', '12:49:59', '12:54:59', '12:59:59', '13:04:59', '13:09:59', '13:14:59', '13:19:59',
               '13:24:59', '13:29:59', '13:34:59', '13:39:59', '13:44:59', '13:49:59', '13:54:59', '13:59:59',
               '14:04:59', '14:09:59', '14:14:59', '14:19:59', '14:24:59', '14:29:59', '14:34:59', '14:39:59',
               '14:44:59', '14:49:59', '14:54:59', '14:59:59', '15:04:59', '15:09:59', '15:14:59', '15:19:59',
               '15:24:59', '15:29:59', '15:34:59', '15:39:59', '15:44:59', '15:49:59', '15:54:59', '15:59:59',
               '16:04:59', '16:09:59', '16:14:59', '16:19:59', '16:24:59', '16:29:59', '16:34:59', '16:39:59',
               '16:44:59', '16:49:59', '16:54:59', '16:59:59', '17:04:59', '17:09:59', '17:14:59', '17:19:59',
               '17:24:59', '17:29:59', '17:34:59', '17:39:59', '17:44:59', '17:49:59', '17:54:59', '17:59:59',
               '18:04:59', '18:09:59', '18:14:59', '18:19:59', '18:24:59', '18:29:59', '18:34:59', '18:39:59',
               '18:44:59', '18:49:59', '18:54:59', '18:59:59', '19:04:59', '19:09:59', '19:14:59', '19:19:59',
               '19:24:59', '19:29:59', '19:34:59', '19:39:59', '19:44:59', '19:49:59', '19:54:59', '19:59:59',
               '20:04:59', '20:09:59', '20:14:59', '20:19:59', '20:24:59', '20:29:59', '20:34:59', '20:39:59',
               '20:44:59', '20:49:59', '20:54:59', '20:59:59', '21:04:59', '21:09:59', '21:14:59', '21:19:59',
               '21:24:59', '21:29:59', '21:34:59', '21:39:59', '21:44:59', '21:49:59', '21:54:59', '21:59:59',
               '22:04:59', '22:09:59', '22:14:59', '22:19:59', '22:24:59', '22:29:59', '22:34:59', '22:39:59',
               '22:44:59', '22:49:59', '22:54:59', '22:59:59', '23:04:59', '23:09:59', '23:14:59', '23:19:59',
               '23:24:59', '23:29:59', '23:34:59', '23:39:59', '23:44:59', '23:49:59', '23:54:59', '23:59:59']
blockid_5m = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
              29, 30, 31, 32, 33, 34, 35, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012,
              1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029,
              1030, 1031, 1032, 1033, 1034, 1035, 1036, 1037, 1038, 1039, 1040, 1041, 1042, 1043, 1044, 1045, 1046,
              1047, 1048, 1049, 1050, 1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059, 1060, 1061, 1062, 1063,
              1064, 1065, 1066, 1067, 1068, 1069, 1070, 1071, 1072, 1073, 1074, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
              46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 1084, 1085,
              1086, 1087, 1088, 1089, 1090, 1091, 1092, 1093, 1094, 1095, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
              80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104,
              105, 106, 107, 108, 109, 110, 1096, 1097, 1098, 1099, 1100, 1101, 1102, 1103, 1104, 111, 112, 113, 114,
              115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135,
              136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156,
              157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177,
              178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191]
time_range_5m = pd.DataFrame({'start_time': pd.to_datetime(start_time_5m).time,
                              'end_time': pd.to_datetime(end_time_5m).time, 'block_id': blockid_5m})

start_time_15m = [
    '00:00:00', '00:15:00', '00:30:00', '00:45:00', '01:00:00', '01:15:00', '01:30:00', '01:45:00',
    '02:00:00', '02:15:00', '02:30:00', '02:45:00',
    '03:00:00', '03:15:00', '03:30:00', '03:45:00', '04:00:00', '04:15:00', '04:30:00', '04:45:00',
    '05:00:00', '05:15:00', '05:30:00', '05:45:00',
    '06:00:00', '06:15:00', '06:30:00', '06:45:00', '07:00:00', '07:15:00', '07:30:00', '07:45:00',
    '08:00:00', '08:15:00', '08:30:00', '08:45:00',
    '09:00:00',
    '09:15:00', '09:30:00', '09:45:00', '10:00:00', '10:15:00', '10:30:00',
    '10:45:00', '11:00:00', '11:15:00', '11:30:00', '11:45:00',
    '12:00:00', '12:15:00', '12:30:00', '12:45:00',
    '13:00:00', '13:15:00', '13:30:00', '13:45:00',
    '14:00:00', '14:15:00', '14:30:00', '14:45:00', '15:00:00', '15:15:00', '15:30:00', '15:45:00', '16:00:00',
    '16:15:00', '16:30:00', '16:45:00', '17:00:00',
    '17:15:00', '17:30:00', '17:45:00', '18:00:00', '18:15:00', '18:30:00', '18:45:00', '19:00:00',
    '19:15:00', '19:30:00', '19:45:00', '20:00:00', '20:15:00', '20:30:00', '20:45:00', '21:00:00', '21:15:00',
    '21:30:00', '21:45:00', '22:00:00', '22:15:00', '22:30:00', '22:45:00', '23:00:00', '23:15:00', '23:30:00',
    '23:45:00']
end_time_15m = [
    '00:14:59', '00:29:59', '00:44:59', '00:59:59', '01:14:59', '01:29:59', '01:44:59', '01:59:59', '02:14:59',
    '02:29:59', '02:44:59', '02:59:59',
    '03:14:59', '03:29:59', '03:44:59', '03:59:59',
    '04:14:59', '04:29:59', '04:44:59', '04:59:59',
    '05:14:59', '05:29:59', '05:44:59', '05:59:59',
    '06:14:59', '06:29:59', '06:44:59', '06:59:59',
    '07:14:59', '07:29:59', '07:44:59', '07:59:59',
    '08:14:59', '08:29:59', '08:44:59', '08:59:59',
    '09:14:59', '09:29:59', '09:44:59', '09:59:59', '10:14:59', '10:29:59', '10:44:59',
    '10:59:59', '11:14:59', '11:29:59', '11:44:59', '11:59:59',
    '12:14:59', '12:29:59', '12:44:59', '12:59:59',
    '13:14:59', '13:29:59', '13:44:59', '13:59:59',
    '14:14:59', '14:29:59', '14:44:59', '14:59:59', '15:14:59', '15:29:59', '15:44:59', '15:59:59',
    '16:14:59', '16:29:59', '16:44:59', '16:59:59',
    '17:14:59', '17:29:59', '17:44:59', '17:59:59', '18:14:59', '18:29:59', '18:44:59', '18:59:59', '19:14:59',
    '19:29:59', '19:44:59', '19:59:59', '20:14:59', '20:29:59', '20:44:59', '20:59:59', '21:14:59', '21:29:59',
    '21:44:59', '21:59:59', '22:14:59', '22:29:59', '22:44:59', '22:59:59', '23:14:59', '23:29:59', '23:44:59',
    '23:59:59']
blockid_15m = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012,
    1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023, 1024,
    12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
    1025, 1026, 1027, 1028,
    23, 24, 25, 26, 27, 28,
    29, 30, 31, 32, 33, 34, 35, 36,
    1029, 1030, 1031,
    37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,
    56, 57, 58, 59, 60, 61, 62, 63]
time_range_15m = pd.DataFrame({'start_time': pd.to_datetime(start_time_15m).time,
                               'end_time': pd.to_datetime(end_time_15m).time, 'block_id': blockid_15m})

start_time_30m = [
    '00:15:00', '00:45:00', '01:15:00', '01:45:00', '02:15:00', '02:45:00', '09:30:00', '10:00:00', '10:30:00',
    '11:00:00', '11:30:00',
    '12:00:00', '12:30:00',
    '13:00:00', '13:30:00', '14:00:00', '14:30:00', '15:00:00', '15:30:00', '16:00:00', '16:30:00',
    '17:15:00', '17:45:00', '18:15:00', '18:45:00', '19:15:00', '19:45:00', '20:15:00', '20:45:00', '21:15:00',
    '21:45:00', '22:15:00', '22:45:00', '23:15:00', '23:45:00', '00:00:00']
end_time_30m = [
    '00:44:59', '01:14:59', '01:44:59', '02:14:59', '02:44:59', '09:29:59', '09:59:59', '10:29:59', '10:59:59',
    '11:29:59', '11:59:59',
    '12:29:59', '12:59:59',
    '13:29:59', '13:59:59', '14:29:59', '14:59:59', '15:29:59', '15:59:59', '16:29:59', '17:14:59',
    '17:44:59', '18:14:59', '18:44:59', '19:14:59', '19:44:59', '20:14:59', '20:44:59', '21:14:59', '21:44:59',
    '22:14:59', '22:44:59', '23:14:59', '23:44:59', '23:59:59', '00:14:59']
blockid_30m = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
    1000, 1001,
    11, 12, 13, 14, 15, 16, 17, 1002, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
    29, 30, 31, 31]
time_range_30m = pd.DataFrame({'start_time': pd.to_datetime(start_time_30m).time,
                               'end_time': pd.to_datetime(end_time_30m).time, 'block_id': blockid_30m})

start_time_1h = [
    '00:15:00', '01:15:00', '02:15:00', '09:30:00', '10:30:00', '11:30:00', '13:30:00', '14:30:00', '15:30:00',
    '16:30:00',
    '17:15:00', '18:15:00', '19:15:00', '20:15:00', '21:15:00', '22:15:00', '23:15:00', '00:00:00']
end_time_1h = [
    '01:14:59', '02:14:59', '09:29:59', '10:29:59', '11:29:59', '13:29:59', '14:29:59', '15:29:59', '16:29:59',
    '17:14:59',
    '18:14:59', '19:14:59', '20:14:59', '21:14:59', '22:14:59', '23:14:59', '23:59:59', '00:14:59']
blockid_1h = [0, 1, 2, 3, 4, 5, 6, 7, 8, 1000, 9, 10, 11, 12, 13, 14, 15, 15]
time_range_1h = pd.DataFrame({'start_time': pd.to_datetime(start_time_1h).time,
                              'end_time': pd.to_datetime(end_time_1h).time, 'block_id': blockid_1h})

start_time_2h = [
    '01:15:00', '09:30:00', '11:30:00', '14:30:00', '16:30:00', '17:15:00', '19:15:00', '21:15:00', '23:15:00',
    '00:00:00']
end_time_2h = [
    '09:29:59', '11:29:59', '14:29:59', '16:29:59', '17:14:59', '19:14:59', '21:14:59', '23:14:59', '23:59:00',
    '01:14:59']

blockid_2h = [0, 1, 2, 3, 1000, 4, 5, 6, 7, 7]
time_range_2h = pd.DataFrame({'start_time': pd.to_datetime(start_time_2h).time,
                              'end_time': pd.to_datetime(end_time_2h).time, 'block_id': blockid_2h})

start_time_3h = ['02:15:00', '11:30:00', '15:30:00', '16:30:00', '17:15:00', '20:15:00', '23:15:00', '00:00:00']
end_time_3h = ['11:29:59', '15:29:59', '16:29:59', '17:14:59', '20:14:59', '23:14:59', '23:59:59', '02:14:59']
blockid_3h = [0, 1, 2, 1000, 3, 4, 5, 5]
time_range_3h = pd.DataFrame({'start_time': pd.to_datetime(start_time_3h).time,
                              'end_time': pd.to_datetime(end_time_3h).time, 'block_id': blockid_3h})

start_time_4h = ['01:15:00', '11:30:00', '16:30:00', '17:15:00', '21:15:00', '00:00:00']
end_time_4h = ['11:29:59', '16:29:59', '17:14:59', '21:14:59', '23:59:59', '01:14:59']
blockid_4h = [0, 1, 1000, 2, 4, 4]
time_range_4h = pd.DataFrame({'start_time': pd.to_datetime(start_time_4h).time,
                              'end_time': pd.to_datetime(end_time_4h).time, 'block_id': blockid_4h})


def deal_tick_data(data):
    """
    该函数仅适用于futu api的mhi数据
    :param data: 要求有code,data_date('2023-08-08'),data_time('20:01:03'),last_price(18998),volume(5607),position(30376)
    :return: 返回trade_time,open/close/high/low/volume/total_volume/position
    """
    price_data = data['last_price']
    open = float(price_data.iloc[0])
    high = float(max(price_data))
    low = float(min(price_data))
    close = float(price_data.iloc[-1])
    volume_data = data['volume']
    total_volume = volume_data.iloc[-1]
    volume = volume_data.iloc[-1] - volume_data.iloc[0]
    position = data['position'].iloc[-1]
    data_hms = pd.to_datetime(data['data_time'].iloc[0])
    data_hour_minute = data_hms.strftime('%H:%M')
    trade_time = data['data_date'].iloc[0] + " " + data_hour_minute + ':00'
    ts_code = data['code'].iloc[0]
    info_df = pd.DataFrame(
        {'ts_code': ts_code, 'trade_time': trade_time, 'open': open, 'high': high, 'low': low, 'close': close,
         'volume': volume, 'total_volume': total_volume, 'position': position}, index=[0])

    return info_df


def trans_difffreq_data(data, block_time):
    """
    该函数仅适用于futu api的mhi数据
    :param data: 要求有'code','trade_time','open','close','high','low','volume','turnover', 'last_close'
    :param block_time: 要求为date.time格式，指数据所在的time_range，如对于5m数据，14:14:00的block_time为14:10:00
    :return: 返回trade_time,open/close/high/low/volume/total_volume/position
    """
    open = float(data['open'].iloc[0])
    high = float(max(data['high']))
    low = float(min(data['low']))
    close = float(data['close'].iloc[-1])
    last_close = float(data['last_close'].iloc[-1])
    volume = sum(data['volume'])
    turnover = data['turnover'].iloc[-1]
    trade_date = pd.to_datetime(data['trade_time'].iloc[0]).strftime('%Y-%m-%d')
    trade_time = trade_date + ' ' + block_time.strftime('%H:%M:00')
    code = data['code'].iloc[0]
    info_df = pd.DataFrame(
        {'code': code, 'trade_time': trade_time, 'open': open, 'close': close, 'high': high, 'low': low,
         'volume': volume,
         'turnover': turnover, 'last_close': last_close}, index=[0])

    return info_df


q_tick = queue.Queue()
q_1m = queue.Queue()
q_5m = queue.Queue()
q_15m = queue.Queue()
q_30m = queue.Queue()
q_1h = queue.Queue()
q_2h = queue.Queue()
q_3h = queue.Queue()
q_4h = queue.Queue()


def deal_data_tick():
    da_tick = pd.DataFrame()
    while True:
        spec_tick = q_tick.get()
        try:
            publish_kline.pubkline(data=spec_tick, ktype=Interval.TICK)
            da_tick = pd.concat([da_tick, spec_tick])
            if len(da_tick.index) > 2000:
                da_tick_sql = da_tick[[
                    'code', 'time', 'price', 'volume', 'turnover', 'ticker_direction', 'type', 'push_data_type']]
                da_tick_sql.to_sql(name='mhi_tick', con=conn_to_sql, index=False, if_exists="append")
                da_tick = pd.DataFrame()

        except Exception:
            logger.wechat(title="!query_mhimain ERROR!", content=traceback.format_exc(), level='error')


def deal_data_1m(queue_put_list=None):
    pre_trade_time = np.nan
    pre_trade_minute = np.nan
    data_1m = pd.DataFrame()
    send_stoptrade_message = 0

    while True:
        data_tick = q_1m.get()
        try:
            trade_time = pd.to_datetime(data_tick['time_key'].iloc[0]) - datetime.timedelta(minutes=1)
            trade_minute = trade_time.strftime('%M')

            if pd.isna(pre_trade_minute):
                data_1m = data_tick
                pre_trade_time = trade_time
                pre_trade_minute = trade_minute
            elif pre_trade_minute == trade_minute:
                data_1m = data_tick
            else:
                data_1m = data_1m.rename(columns={'time_key': 'trade_time'})
                data_1m['trade_time'] = pre_trade_time
                data_1m_sql = data_1m[
                    ['code', 'trade_time', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'last_close']]

                # 判断是否无交易，以解决futu在无交易时仍然传数据的问题
                if data_1m_sql['volume'].iloc[0] != 0:
                    if_no_trade = False
                    send_stoptrade_message = 0
                else:
                    # 用恒指判断（futu没有提供其他衍生品的市场状态）
                    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
                    market_status = quote_ctx.get_market_state(code)[1]['market_state'].iloc[0]

                    quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽
                    stop_status = ['NONE']

                    # 当交易数量为0且市场为none时，不生成数据
                    if market_status in stop_status:
                        if_no_trade = True
                        send_stoptrade_message += 1
                    else:
                        if_no_trade = False
                        send_stoptrade_message = 0

                # 当交易量不为0或者market_status不为停盘时，传出数据
                if not if_no_trade:
                    data_1m_sql.to_sql(name='mhi_1m', con=conn_to_sql, index=False, if_exists="append")
                    conn_to_sql.clear_compiled_cache()

                    data_1m_sql['trade_time'] = data_1m_sql['trade_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    ktype = Interval.K_1M
                    publish_kline.pubkline(data=data_1m_sql, ktype=ktype)

                    if queue_put_list is None:
                        pass
                    else:
                        for queue_put in queue_put_list:
                            queue_put.put(data_1m_sql)
                # 如果交易量为0且市场状态为NONE,发送消息（且只发一次）
                else:
                    if send_stoptrade_message == 1:
                        logger.info(f'code:{code} No TRADING')

                pre_trade_minute = trade_minute
                pre_trade_time = trade_time
                data_1m = data_tick

        except Exception as e:
            logger.wechat(title="!query_mhimain ERROR!", content=traceback.format_exc(), level='error')


def deal_difffreq_data(ktype, table_name, time_range, queue_get, queue_put_list=None):
    """

    :param queue_put_list:
    :param queue_get:传入的必须是分钟数据
    :param table_name:
    :param time_range:
    :return:
    freq1为频率最低的数据，freq2为频率较高的数据，如用该函数获取5m数据，则data_freq1指的是1m数据，data_freq2指的是5m数据
    """
    # 初始化变量

    pre_block_id_freq2 = np.nan
    pre_block_time = np.nan
    pre_date = np.nan
    data_freq2 = pd.DataFrame()

    while True:
        data_freq1 = queue_get.get()
        try:
            trade_time = pd.to_datetime(data_freq1['trade_time'].iloc[0]).time()
            trade_date = pd.to_datetime(data_freq1['trade_time'].iloc[0]).date()
            # 确定当前数据所在的block_id
            cond_start_freq2 = time_range['start_time'] <= trade_time
            cond_end_freq2 = time_range['end_time'] >= trade_time
            spec_time_range_freq2 = time_range.where(
                cond_start_freq2 & cond_end_freq2, inplace=False).dropna().iloc[[0]]
            block_id_freq2 = spec_time_range_freq2['block_id'].iloc[0]
            block_time = spec_time_range_freq2['start_time'].iloc[0]

            # 用于判断是否为特定block_id的最后一分钟数据
            trade_hm = trade_time.strftime('%H:%M')
            block_end_time = spec_time_range_freq2['end_time'].iloc[0].strftime('%H:%M')

            if block_id_freq2 >= 1000:
                logger.wechat(title="!query_mhimain ERROR!",
                              content=f'table_name:{table_name} The exchange extends trading hours', level='error')

            # 若第一次运行该函数，如在9：14第一次运行该函数，则在freq2下，默认9：14的数据为9：10的开始数据
            if pd.isna(pre_block_id_freq2):
                data_freq2 = data_freq1
                pre_block_id_freq2 = block_id_freq2
                pre_block_time = block_time
                pre_date = trade_date

            # 传入的分钟数据为block_id的最后一分钟，且下一个block_id不相同
            elif trade_hm == block_end_time:
                block_index = spec_time_range_freq2.index[0]
                last_block_index = time_range.index[-1]
                same_block_id = time_range[time_range['block_id'] == block_id_freq2]  # 与当前block_id相同的数据
                if len(same_block_id) < 2:
                    renew = True
                elif (block_index + 1) != last_block_index:
                    renew = True
                else:
                    renew = False

                if renew:
                    # 如果刚好之前没有数据,则data_freq2_sql为当前数据
                    if pd.isna(pre_block_id_freq2):
                        data_freq2_sql = data_freq1
                    else:
                        data_freq2 = pd.concat([data_freq2, data_freq1])
                        data_freq2_sql = trans_difffreq_data(data=data_freq2, block_time=pre_block_time)
                    data_freq2_sql.to_sql(name=table_name, con=conn_to_sql, index=False, if_exists="append")
                    # 传递到cache
                    publish_kline.pubkline(data=data_freq2_sql, ktype=ktype)

                    # 传导到下一个频率
                    if queue_put_list is None:
                        pass
                    else:
                        for queue_put in queue_put_list:
                            queue_put.put(data_freq2_sql)

                    # 重置信息
                    pre_block_id_freq2 = np.nan
                    pre_block_time = np.nan
                    pre_date = np.nan
                    data_freq2 = pd.DataFrame()
                else:
                    data_freq2 = pd.concat([data_freq2, data_freq1])

            # 若前一block_id和当前block_id相同，则只添加数据
            elif pre_block_id_freq2 == block_id_freq2:
                pre_date = pd.to_datetime(data_freq2['trade_time'].iloc[-1]).date()

                date_lag = (trade_date - pre_date).days
                # 更新pre_date，防止出现不停传入新日期
                pre_date = trade_date
                if date_lag <= 2:
                    data_freq2 = pd.concat([data_freq2, data_freq1])
                else:
                    # 进行数据整理，并写入
                    data_freq2_sql = trans_difffreq_data(data=data_freq2, block_time=pre_block_time)
                    data_freq2_sql.to_sql(name=table_name, con=conn_to_sql, index=False, if_exists="append")

                    # 传递到cache
                    publish_kline.pubkline(data=data_freq2_sql, ktype=ktype)

                    # 传导到下一个频率
                    if queue_put_list is None:
                        pass
                    else:
                        for queue_put in queue_put_list:
                            queue_put.put(data_freq2_sql)

                    # 重置信息
                    data_freq2 = data_freq1
                    pre_block_id_freq2 = block_id_freq2
                    pre_block_time = block_time

            # 若前一block_id和当前block_id不同，则将前一block_id的所有数据写入
            elif pre_block_id_freq2 != block_id_freq2:
                # 进行数据整理，并写入
                data_freq2_sql = trans_difffreq_data(data=data_freq2, block_time=pre_block_time)
                data_freq2_sql.to_sql(name=table_name, con=conn_to_sql, index=False, if_exists="append")

                # 传递到cache
                publish_kline.pubkline(data=data_freq2_sql, ktype=ktype)

                # 传导到下一个频率
                if queue_put_list is None:
                    pass
                else:
                    for queue_put in queue_put_list:
                        queue_put.put(data_freq2_sql)

                # 重置信息
                data_freq2 = data_freq1
                pre_block_id_freq2 = block_id_freq2
                pre_block_time = block_time

        except Exception:
            logger.wechat(title="!query_mhimain ERROR!", content=traceback.format_exc(), level='error')


class TickerTest(TickerHandlerBase):
    def on_recv_rsp(self, rsp_str):
        ret_code, data = super(TickerTest, self).on_recv_rsp(rsp_str)
        if ret_code != RET_OK:
            logger.wechat(title="!query_mhimain ERROR!",
                          content=f'code:{code} TickerTest ERROR res:{data}', level='error')
            return RET_ERROR, data
        else:
            q_tick.put(data)


class CurKlineTest(CurKlineHandlerBase):
    def on_recv_rsp(self, rsp_str):
        ret_code, data = super(CurKlineTest, self).on_recv_rsp(rsp_str)
        if ret_code != RET_OK:
            logger.wechat(title="!query_mhimain ERROR!",
                          content=f'code:{code} CurKlineTest ERROR res:{data}', level='error')
            return RET_ERROR, data
        else:
            q_1m.put(data)


# quote_1m.close()


class SysNotifyTest(SysNotifyHandlerBase):
    def on_recv_rsp(self, rsp_str):
        ret_code, data = super(SysNotifyTest, self).on_recv_rsp(rsp_str)
        notify_type, sub_type, msg = data
        if ret_code != RET_OK:
            logger.wechat(title="!query_mhimain ERROR!",
                          content=f'code:{code} SysNotifyTest ERROR res:{data}', level='error')
            return RET_ERROR, data

        logger.info(f"notify_type:{notify_type},sub_type:{sub_type},msg:{msg}")


quote_1m = OpenQuoteContext(host='127.0.0.1', port=11111)


def monitor_ctx_status():
    while True:
        status = quote_1m.status
        if status != 'READY':
            logger.wechat(title="!query_mhimain ERROR!",
                          content="OpenQuoteContext status ERROR", level='error')
        else:
            pass
        time.sleep(10)


def main():
    get_tick = threading.Thread(target=deal_data_tick,
                                name='Thread_tick')
    get_1m = threading.Thread(target=deal_data_1m, kwargs={'queue_put_list': [q_5m, q_15m, q_30m, q_1h, q_2h, q_3h,
                                                                              q_4h]}, name='Thread_1m')
    get_5m = threading.Thread(target=deal_difffreq_data, kwargs={'ktype': Interval.K_5M,
                                                                 'queue_get': q_5m,
                                                                 'table_name': 'mhi_5m', 'time_range': time_range_5m},
                              name='Thread_5m')
    get_15m = threading.Thread(target=deal_difffreq_data,
                               kwargs={'ktype': Interval.K_15M, 'queue_get': q_15m,
                                       'table_name': 'mhi_15m', 'time_range': time_range_15m})
    get_30m = threading.Thread(target=deal_difffreq_data,
                               kwargs={'ktype': Interval.K_30M, 'queue_get': q_30m,
                                       'table_name': 'mhi_30m', 'time_range': time_range_30m})
    get_1h = threading.Thread(target=deal_difffreq_data,
                              kwargs={'ktype': Interval.K_1H, 'queue_get': q_1h,
                                      'table_name': 'mhi_1h', 'time_range': time_range_1h})
    get_2h = threading.Thread(target=deal_difffreq_data,
                              kwargs={'ktype': Interval.K_2H, 'queue_get': q_2h,
                                      'table_name': 'mhi_2h', 'time_range': time_range_2h})
    get_3h = threading.Thread(target=deal_difffreq_data,
                              kwargs={'ktype': Interval.K_3H, 'queue_get': q_3h,
                                      'table_name': 'mhi_3h', 'time_range': time_range_3h})
    get_4h = threading.Thread(target=deal_difffreq_data,
                              kwargs={'ktype': Interval.K_4H, 'queue_get': q_4h,
                                      'table_name': 'mhi_4h', 'time_range': time_range_4h})

    get_tick.start()
    get_1m.start()
    get_5m.start()
    get_15m.start()
    get_30m.start()
    get_1h.start()
    get_2h.start()
    get_3h.start()
    get_4h.start()

    quote_tick = OpenQuoteContext(host='127.0.0.1', port=11111)
    handler_tick = TickerTest()
    quote_tick.set_handler(handler_tick)  # 设置实时报价回调
    quote_tick.subscribe([code], [SubType.TICKER])  # 订阅实时报价类型，FutuOpenD 开始持续收到服务器的推送
    # quote_tick.close()  # 关闭当条连接，FutuOpenD 会在1分钟后自动取消相应股票相应类型的订阅

    handler_1m = CurKlineTest()
    quote_1m.set_handler(handler_1m)  # 设置实时报价回调
    quote_1m.subscribe([code], [SubType.K_1M])  # 订阅实时报价类型，FutuOpenD 开始持续收到服务器的推送

    sys_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    handler = SysNotifyTest()
    sys_ctx.set_handler(handler)

    t_monitor_ctx_status = threading.Thread(target=monitor_ctx_status,
                                            name='Thread_monitor')
    t_monitor_ctx_status.start()


if __name__ == '__main__':
    main()
