import logging  # 引入logging模块
import os.path
import time

# 第一步，创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Log等级总开关
# 第二步，创建一个handler，用于写入日志文件
rq = time.strftime('%Y%m%d', time.localtime(time.time()))
# print(os.getcwd())
# print(os.path.dirname(os.getcwd()))
log_path = os.getcwd() + '\\Logs\\'
print('log_path:',log_path)
isExists = os.path.exists(log_path)
# 判断结果
if not isExists:
    # 如果不存在则创建目录
    # 创建目录操作函数
    os.makedirs(log_path)
log_name = log_path + rq + '.log'
logfile = log_name
fh = logging.FileHandler(logfile, mode='a')
fh.setLevel(logging.DEBUG)  # 输出到file的log等级的开关
# 第三步，定义handler的输出格式
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
# 第四步，将logger添加到handler里面
logger.addHandler(fh)
print("in logging init...")
logger.debug('logging init over')

# class myLogging():
#     def __init__(self):
#         # 第一步，创建一个logger
#         self.logger = logging.getLogger()
#         self.logger.setLevel(logging.DEBUG)  # Log等级总开关
#         # 第二步，创建一个handler，用于写入日志文件
#         rq = time.strftime('%Y%m%d', time.localtime(time.time()))
#         # print(os.getcwd())
#         # print(os.path.dirname(os.getcwd()))
#         log_path = os.getcwd() + '\\Logs\\'
#         print('log_path:',log_path)
#         isExists = os.path.exists(log_path)
#         # 判断结果
#         if not isExists:
#             # 如果不存在则创建目录
#             # 创建目录操作函数
#             os.makedirs(log_path)
#         log_name = log_path + rq + '.log'
#         logfile = log_name
#         fh = logging.FileHandler(logfile, mode='a')
#         fh.setLevel(logging.DEBUG)  # 输出到file的log等级的开关
#         # 第三步，定义handler的输出格式
#         formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
#         fh.setFormatter(formatter)
#         # 第四步，将logger添加到handler里面
#         self.logger.addHandler(fh)
#         print("in logging init...")
#         self.logger.debug('logging init over')
#
#     def debug(self,msg):
#         self.logger.debug(msg)
#
#     def info(self,msg):
#         self.logger.info(msg)
#
#     def warning(self,msg):
#         self.logger.warning(msg)
#
#     def error(self,msg):
#         self.logger.error(msg)
#
#     def critical(self,msg):
#         self.logger.critical(msg)