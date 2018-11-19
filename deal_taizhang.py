 # _*_ coding=utf-8
# 台账相关处理函数
# getCommunity：将每一户的数据和所有账页数据传给taizhang函数
# taizhang：按年将数据划分，处理每一年的数据，使用正则匹配对应指标的所有数据传给handle_data
# handle_data：处理传入的某一类别记录的所有

import pandas as pd
# import math
from decimal import Decimal
import re
import json
import sys,os
import myLogging as mylogger
# from myLogging import myLogging

class deal_taizhang():
    def __init__(self):
        mylogger.logger.debug("deal_taizhang init..")

        # self.tz_write("第二行\n")
        # col = ['指标','12月','01月','02月','03月','04月','05月','06月','07月','08月','09月','10月','11月','合计']
        # self.result = ''
        data = {'指标':[],'12月':[],'01月':[],'02月':[],'03月':[],'04月':[],'05月':[],'06月':[],'07月':[],'08月':[],'09月':[],'10月':[],'11月':[],'合计':[]}
        self.result = pd.DataFrame(data)
        self.result = self.result[['指标','12月','01月','02月','03月','04月','05月','06月','07月','08月','09月','10月','11月','合计']]


        # self.zhibiao = {
        #       "210111": "\u6708\u5de5\u8d44",
        #       "331": "  #\u79df\u8d41\u623f\u623f\u79df",
        #       "220611": "\u4ea4\u901a\u8fd0\u8f93.\u4ed3\u50a8\u548c\u90ae\u653f\u4e1a\u6536\u5165",
        #       "220711": "\u4f4f\u5bbf\u548c\u9910\u996e\u4e1a\u6536\u5165",
        #       "220811": "\u623f\u5730\u4ea7\u4e1a\u6536\u5165",
        #       "250261": "  #\u8c03\u67e5\u8865\u8d34",
        #       "333305": "  #\u7ba1\u9053\u5929\u7136\u6c14",
        #       "532211": "\u4e2a\u4eba\u7f34\u7eb3\u7684\u533b\u7597\u4fdd\u9669",
        #       "2403": "\u653f\u7b56\u6027\u751f\u4ea7\u8865\u8d34",
        #       "33": "3.\u5c45\u4f4f\u7c7b\u652f\u51fa",
        #       "166311": "\u81ea\u4ea7\u81ea\u7528\u5c45\u4f4f\u7c7b\u8349\u6d88\u8d39 (\u516c\u62c5)",
        #       "333308": "  #\u7f50\u88c5\u6db2\u5316\u77f3\u6cb9\u6c14",
        #       "2407": "\u8d61\u517b\u6536\u5165",
        #       "221011": "\u5c45\u6c11\u670d\u52a1\u3001\u4fee\u7406\u548c\u5176\u4ed6\u670d\u52a1\u4e1a",
        #       "311": "  #\u98df\u54c1\u6d88\u8d39\u652f\u51fa",
        #       "230511": "\u51fa\u79df\u623f\u5c4b\u51c0\u6536\u5165",
        #       "32": "2.\u8863\u7740\u7c7b\u652f\u51fa",
        #       "26": "\u501f\u8d37\u6027\u6240\u5f97",
        #       "333211": "  #\u751f\u6d3b\u7528\u7535",
        #       "2405": "\u62a5\u9500\u533b\u7597\u8d39",
        #       "221111": "\u5176\u4ed6\u975e\u519c\u884c\u4e1a\u7ecf\u8425\u6536\u5165",
        #       "133": "\u755c\u7267\u4e1a\u751f\u4ea7\u6210\u672c",
        #       "210291": "\u5176\u4ed6\u52b3\u52a8\u6240\u5f97",
        #       "220411": "\u5efa\u7b51\u4e1a\u6536\u5165",
        #       "9999": "\u751f\u6d3b\u6d88\u8d39\u652f\u51fa",
        #       "121": "\u519c\u4e1a\u751f\u4ea7\u7ecf\u8425\u6536\u5165",
        #       "132": "\u6797\u4e1a\u751f\u4ea7\u6210\u672c",
        #       "240191": "\u5176\u4ed6\u517b\u8001\u91d1",
        #       "220911": "\u79df\u8d41\u548c\u5546\u52a1\u670d\u52a1\u4e1a\u6536\u5165",
        #       "539911": "\u5176\u4ed6\u7ecf\u5e38\u8f6c\u79fb\u652f\u51fa",
        #       "2406": "\u5916\u51fa\u4ece\u4e1a\u4eba\u5458\u5bc4\u56de\u5e26\u56de\u6536\u5165",
        #       "122": "\u6797\u4e1a\u751f\u4ea7\u7ecf\u8425\u6536\u5165",
        #       "131": "\u519c\u4e1a\u751f\u4ea7\u6210\u672c",
        #       "240131": "\u65b0\u578b\u519c\u6751\u517b\u8001\u4fdd\u9669\u91d1",
        #       "210221": "\u81ea\u7531\u804c\u4e1a\u52b3\u52a8\u6240\u5f97",
        #       "36": "6.\u6559\u80b2\u6587\u5316\u5a31\u4e50\u652f\u51fa",
        #       "16302": "\u81ea\u4ea7\u81ea\u7528\u5bb6\u79bd\u6d88\u8d39 (kg)",
        #       "240121": "\uff08\u57ce\u9547\uff09\u5c45\u6c11\u793e\u4f1a\u517b\u8001\u4fdd\u9669\u91d1",
        #       "3": "\u5168\u90e8\u751f\u6d3b\u6d88\u8d39\u652f\u51fa",
        #       "210121": "\u8865\u53d1\u5de5\u8d44",
        #       "220511": "\u6279\u53d1\u548c\u96f6\u552e\u4e1a\u6536\u5165",
        #       "2402": "\u793e\u4f1a\u6551\u52a9\u548c\u8865\u52a9",
        #       "123": "\u755c\u7267\u4e1a\u751f\u4ea7\u7ecf\u8425\u6536\u5165",
        #       "31": "1.\u98df\u54c1\u70df\u9152\u6d88\u8d39\u652f\u51fa",
        #       "124": "\u6e14\u4e1a\u751f\u4ea7\u7ecf\u8425\u6536\u5165",
        #       "240111": "\u79bb\u9000\u4f11\u4eba\u5458\u517b\u8001\u91d1",
        #       "532311": "\u4e2a\u4eba\u7f34\u7eb3\u7684\u5931\u4e1a\u4fdd\u9669",
        #       "333306": "  #\u7ba1\u9053\u7164\u6c14",
        #       "2408": "\u5176\u4ed6\u7ecf\u5e38\u6027\u8f6c\u79fb\u6536\u5165",
        #       "532411": "\u4e2a\u4eba\u7f34\u7eb3\u7684\u4f4f\u623f\u516c\u79ef\u91d1",
        #       "25": "\u975e\u6536\u5165\u6240\u5f97",
        #       "210131": "\u4e0d\u6309\u6708\u53d1\u653e\u7684\u5de5\u8d44",
        #       "532111": "\u4e2a\u4eba\u7f34\u7eb3\u7684\u517b\u8001\u4fdd\u9669",
        #       "230411": "\u8f6c\u8ba9\u627f\u5305\u571f\u5730\u7ecf\u8425\u6743\u79df\u91d1\u51c0\u6536\u5165",
        #       "16101": "\u81ea\u4ea7\u81ea\u7528\u8c37\u7269\u6d88\u8d39 (kg)",
        #       "34": "4.\u751f\u6d3b\u7528\u54c1\u53ca\u670d\u52a1\u652f\u51fa",
        #       "220211": "\u5236\u9020\u4e1a\u6536\u5165",
        #       "134": "\u6e14\u4e1a\u751f\u4ea7\u6210\u672c",
        #       "333111": "  #\u751f\u6d3b\u7528\u6c34",
        #       "333307": "  #\u7ba1\u9053\u6db2\u5316\u77f3\u6cb9\u6c14",
        #       "38": "8.\u5176\u4ed6\u7528\u54c1\u53ca\u670d\u52a1\u652f\u51fa",
        #       "37": "7.\u533b\u7597\u4fdd\u5065\u652f\u51fa",
        #       "16303": "\u81ea\u4ea7\u81ea\u7528\u86cb\u7c7b\u6d88\u8d39 (kg)",
        #       "35": "5.\u4ea4\u901a\u901a\u4fe1\u7c7b\u652f\u51fa",
        #       "531": "\u4ea4\u7eb3\u6240\u5f97\u7a0e",
        #       "16109": "\u81ea\u4ea7\u81ea\u7528\u852c\u83dc\u53ca\u98df\u7528\u83cc\u6d88\u8d39 (kg)",
        #       "230211": "\u96c6\u4f53\u5206\u914d\u7684\u7ea2\u5229"
        # }
        # print(os.getcwd())
        mylogger.logger.debug("deal_taizhang init ok")
        # self.tz.write(os.getcwd())
        zhibiao_path = os.getcwd() + '\\zhibiao_code.json'
        with open(zhibiao_path, 'r',encoding='utf-8') as f:
            self.zhibiao = json.load(f)

    def tz_write(self,msg):
        with open('输出台账.txt','a') as f:
            f.write(msg)
    def insert_to_pd(self,data):
        self.result = self.result.append(data, ignore_index=True)

    # 打开csv文件 返回DataFrame对象
    def read_csv(self, path):
        with open(path, 'r') as f:
            # print("open file")
            mylogger.logger.debug('mainWin>openFile:', path)
            file = pd.read_csv(f, header=0)
        return file

    def member(self,member,code):
        data = member[code]
        if pd.isnull(data) == False:
            if type(data) == type(1.0):
                data = int(data)
            return str(data)
        else:
            return "空值"
    def addTableAData(self,family):
        family.fillna(0)
        dict = {}


        for index,member in family.iterrows():
            # person = member["A100"]
            # person_name = member["A101"]
            retire_code = member["A201"]
            if pd.isnull(retire_code) == False:
                retire_code = int(retire_code)
                if retire_code == 1:
                    retire = "行政事业单位离退休人员"
                elif retire_code == 2:
                    retire = "其他单位离退休人员"
                elif retire_code == 3:
                    retire = "非离退休人员"
                else:
                    retire = '空'
                haslabor = member["A203"]
                if pd.isnull(haslabor) == False:
                    if haslabor == 1:
                        employ = "丧失劳动能力"
                    else:
                        employ_code = member["A204"]
                        if pd.isnull(employ_code) == False:
                            employ_code = int(employ_code)
                            if employ_code == 1:
                                try:
                                    department_code = int(member["A205"])
                                    if department_code == 1:
                                        employ = "雇主"
                                    elif department_code == 2:
                                        employ = "公职人员"
                                    elif department_code == 3:
                                        employ = "事业单位人员"
                                    elif department_code == 4:
                                        employ = "国有企业雇员"
                                    elif department_code == 5:
                                        employ = "其他雇员"
                                    elif department_code == 6:
                                        employ = "农业自营"
                                    elif department_code == 7:
                                        employ = "非农自营"
                                except :
                                    employ = "A205填写有误"
                                # employ = "本季度从业过"
                            elif employ_code == 2:
                                employ = "未从业"
                        else:
                            employ = "空"
                else:
                    employ_code = member["A204"]
                    if pd.isnull(employ_code) == False:
                        employ_code = int(employ_code)
                        if employ_code == 1:
                            try:
                                department_code = int(member["A205"])
                                if department_code == 1:
                                    employ = "雇主"
                                elif department_code == 2:
                                    employ = "公职人员"
                                elif department_code == 3:
                                    employ = "事业单位人员"
                                elif department_code == 4:
                                    employ = "国有企业雇员"
                                elif department_code == 5:
                                    employ = "其他雇员"
                                elif department_code == 6:
                                    employ = "农业自营"
                                elif department_code == 7:
                                    employ = "非农自营"
                            except:
                                employ = "A205填写有误"
                            # employ = "本季度从业过"
                        elif employ_code == 2:
                            employ = "未从业"
                    else:
                        employ = "空"
                if retire != "空" or employ != "空":
                    # dict["指标"] = "[" + str(int(member["A100"])) + "]_" + str(member["A106"]) + "_" + str(member["A101"]) + " " + retire + " " + employ
                    dict["指标"] = "[" + self.member(member,"A100") + "]_" + self.member(member,"A106") + "_" + self.member(member,"A101") + " " + retire + " " + employ
                    self.insert_to_pd(dict)

    def getCommunity(self,communityCode,townTable,TableA,TableD):
        # self.result = pd.DataFrame(columns=["指标","12月","01月","02月","03月","04月","05月","06月","07月","08月","09月","10月","11月","合计"])
        mylogger.logger.debug("deal_taizhang>function:getCommunity")
        # 清空台账结果表
        self.result.drop(self.result.index, inplace=True)

        townTable["VID"] = townTable["VID"].apply(str)
        townTable["VID"] = townTable["VID"].apply(lambda x:x.strip())
        # print("communityCode:",communityCode)
        # print("type:",type(communityCode))
        pattern = communityCode + '(.*)'
        communityFamily = [x for x in TableA["SID"] if re.match(pattern, x)]

        communityFamily = set(communityFamily)
        communityFamily = list(communityFamily)
        communityFamily.sort()
        i = 0
        for familySID in communityFamily:
            # print(familySID)
            # print(familySID[0:15])
            # print(type(townTable["VID"].values[0]),type(familySID[0:15]))
            # 获取相同sid的行即为同一户的成员
            family = TableA[TableA["SID"] == familySID]
            zy_data = TableD[TableD["SID"] == familySID]
            # print(townTable)
            # print(townTable["VID"].values[0],type(townTable["VID"].values[0]),familySID[0:15],type(familySID[0:15]))
            # print(len(townTable["VID"].values[0]), len(familySID[0:15]))
            # print(townTable["VID"].values[0] == familySID[0:15])

            town = townTable[townTable["VID"] == familySID[0:15]]
            # 151116116220002 H00101
            # 310151110208001 H06201
            if zy_data.empty == True:
                continue
            # 按照人码进行排序
            family = family.sort_values(by='A100')
            i += 1
            if town.empty == False:
                text = familySID + " " + town["TOWNNAME"].values[0] + " " + town["VNAME"].values[0]
                info = {"指标": text}
                # info = {"指标": family["SID"].values[0]}
                self.insert_to_pd(info)
            else:
                warning = "找不到户" + familySID + "所在乡镇，请检查数据是否有问题。"
                mylogger.logger.warning(warning)

            dict = {}
            hz_name = self.getHuByPerson(family, 1, "A101")
            if hz_name == "无此项":
                continue
            # family.shape
            text = "[地] 户主：" + hz_name + " 家庭人数：" + str(len(family.index)) + "人 记录笔数"
            dict["指标"] = text

            personSum = 0
            month_arr = zy_data["MONTH"].drop_duplicates()
            for mon in month_arr:
                # mon = mon.strip()
                # print("mon 类型",type(mon),">>>",mon,"<<<")
                key = str(mon).strip().zfill(2) + "月"
                value = len(zy_data[zy_data["MONTH"] == mon].index)
                personSum += value
                dict[key] = int(value)
            dict["合计"] = personSum
            # print(dict)
            self.insert_to_pd(dict)

            self.addTableAData(family)

            # print("SID:",familySID)

            # 处理每一户的数据
            self.taizhang(family, zy_data)
        return self.result


        # print(communityFamily)
        # 将表的数据转置显示，显示效果比较接近数据
        # hu_data = hu_data.T
        # print(hu_data)
        # if i==5:
        #     break
        # print("============####============")
        # print("户：", i)
        # print("SID：", family["SID"].values[0])


    # 将A表中的数据按户进行划分
    def spliteFamily(self,TableA,TableD):
        print("spliteFamily")
        # global zhibiao
        print("openzhibiao")
        # print(TableA)
        # 按照sid区分每一户
        # 先取sid，去掉重复值
        sid_array = TableA["SID"].drop_duplicates()
        # print("1")
        # print(sid_array)
        i = 0
        for sid_index in sid_array:
            # print("2")
            # 获取相同sid的行即为同一户的成员
            hu_data = TableA[TableA["SID"] == sid_index]
            zy_data = TableD[TableD["SID"] == sid_index]
            # 按照人码进行排序
            hu_data = hu_data.sort_values(by='A100')
            # 将表的数据转置显示，显示效果比较接近数据
            # hu_data = hu_data.T
            i += 1
            self.taizhang(hu_data,zy_data)
        # print(TableA["SID"])
        # for row in TableA.iterrows():
        #     print(row[1])

    # 获取某个编码的金额 2
    def moneySum(self,zy, code, person=0):
        # if person != 0:
        #     zy = zy[zy["PERSON"] == person]
        # print("moneyZY",zy,code)
        pattern = code + "(.*)"
        code = [x for x in zy["CODE"] if re.match(pattern, x)]
        if len(code) != 0:
            code = set(code)
            code = list(code)
            code.sort()
            frames = []
            for index in code:
                df = zy[zy["CODE"] == index]
                frames.append(df)
            if len(frames) != 0:
                result = pd.concat(frames)
                # print(result["CODE"],result["MONEY"])
                # return round(sum(result["MONEY"].apply(float)),1)
                return sum(result["MONEY"].apply(Decimal))
        # print("无此记录")
        return 0

    def getHuByPerson(self,hu,person,code):
        person = hu[hu["A100"] == int(person)]
        if person.empty == False:
            return person[code].values[0]
        else:
            return "无此项"


    def getByCode(self,zy,code):
        result = pd.DataFrame()
        pattern = code + "(.*)"
        code = [x for x in zy["CODE"] if re.match(pattern, x)]
        if len(code) != 0:
            code = set(code)
            code = list(code)
            code.sort()
            frames = []
            for index in code:
                df = zy[zy["CODE"] == index]
                frames.append(df)
            if len(frames) != 0:
                result = pd.concat(frames)
        return result
    # 查找指标代码是否存在与指标代码表中，若不存在，则返回其存在与表中的父类
    def findCate(self,index_code):
        # global zhibiao
        if index_code in self.zhibiao:
            return index_code
        else:
            while index_code != '0' and index_code not in self.zhibiao :
                # print("now zhibiao_code:", index_code)
                index_code = str(int(int(index_code) / 10))
        return index_code

    # 判断编码是否有更上层的类别
    def hasParent(self,index_arr):
        index_arr = set(index_arr)
        # print("原始：",index_arr)
        # 当指标代码不存在与指标表中时，则说明其有所属父类
        exist_arr = []
        for index in index_arr:
            index_code = index
            added = False
            while index_code != '0':
                if index_code in self.zhibiao :
                    # print("now zhibiao_code:", index_code)
                    added = True
                    exist_arr.append(index_code)
                    index_code = str(int(int(index_code) / 10))
                else:
                    index_code = str(int(int(index_code) / 10))
            if added == False:
                exist_arr.append(index)
        exist_arr = set(exist_arr)
        exist_arr = list(exist_arr)
        exist_arr.sort()
        # print(exist_arr)
        return exist_arr
    def old_hasParent(self,index_arr):
        index_arr = set(index_arr)
        # print("原始：",index_arr)
        # 当指标代码不存在与指标表中时，则说明其有所属父类
        exist_arr = []
        for index in index_arr:
            if index not in self.zhibiao:
                res = self.findCate(index)
                if(res != '0'):
                    # print("有父类：",res,index)
                    exist_arr.append(res)
                else:
                    exist_arr.append(index)
            else:
                exist_arr.append(index)
        exist_arr = set(exist_arr)
        exist_arr = list(exist_arr)
        exist_arr.sort()
        # print(exist_arr)
        return exist_arr


    # 处理每户的账页数据
    def deal_wage_income(self,hu,zy,wage_code):
        # 将账页表中的所有工资性收入数据取出进行处理
        for wage_index in wage_code:
            month_arr = zy["MONTH"].drop_duplicates()


            # 该户人家下，该编码对应的所有数据
            # wage_income = zy[zy["CODE"] == wage_index]
            wage_income = self.getByCode(zy,wage_index)
            # print(wage_income)
            # wage_income = splite_month_data[splite_month_data["CODE"] == wage_index]
            # 删除重复人码
            person_code = list(wage_income["PERSON"].drop_duplicates())
            person_code.sort()

            if wage_index[0] == "3" or len(wage_index) < 6:
                person_code = ["99"]
            # 将收支情况按人码区分开来
            # 99表示公共开支，有序号的表示该人对应的收支
            for person_index in person_code:
                # 存放每一行将要添加的数据
                dict = {}
                totalSum = 0
                # print("totalSum1==>", totalSum)
                # 人码编号<99表示该项收支是某个具体成员的
                if(int(person_index) < 99):

                    # print("<99")
                    # 取对应人码取所有条数的数据
                    person_income = wage_income[wage_income["PERSON"] == person_index]
                    # 将数据按月划分，计算总和添加到dict中
                    for month_index in month_arr:
                        splite_month_data = person_income[person_income["MONTH"] == month_index]
                        # 将该人码的金额列相加即为对应编码总的工资性收入
                        # person_wage = sum(splite_month_data["MONEY"].apply(float))
                        # print("月份数据",splite_month_data,wage_index)
                        person_wage = self.moneySum(splite_month_data,wage_index)
                        key = str(month_index).strip().zfill(2) + "月"
                        # value = round(person_wage, 1)
                        value = float(person_wage)
                        totalSum = Decimal(str(totalSum)) + Decimal(str(value))
                        # print("value==>",value,"totalSum2==>", totalSum)
                        dict[key] = value
                    person_name = self.getHuByPerson(hu,int(person_index),"A101")
                    if person_name == "无此项":
                        continue

                    res_wage_index = self.findCate(wage_index)
                    # 判断指标编码是否在表中存在
                    if( res_wage_index != '0'):
                        # "类别编码：", wage_index,
                        # print(self.zhibiao[wage_index],"[第",person_index,"人_",person_name, "]:",person_wage)
                        key = "指标"
                        value = "  " + self.zhibiao[res_wage_index] + "[第" + str(person_index) + "人_" + person_name + "]"
                        # self.tz_write(info)
                        dict[key] = value

                        # print("dict",dict)

                    # 若编码在指标列表中不存在，则直接将编码存入
                    else:
                        # print(wage_index,"[第",person_index,"人_",person_name, "]:",person_wage)
                        key = "指标"
                        value = "  " + str(wage_index) + "[第" + str(person_index) + "人_" + person_name +  "]"
                        dict[key] = value
                        # key = str(month_index) + "月"
                        # value = round(person_wage, 1)
                        value = float(person_wage)

                        dict[key] = value
                        # print("dict", dict)
                    # print("info:",info)
                    # tz.write(info)
                    #     self.tz_write(info)
                # 人码编码为99，表示家庭公共开支，不区分具体人
                else:
                    # print('else')
                    # 将该指标的开支按月份分开处理，添加到dict
                    for month_index in month_arr:
                        splite_month_data = wage_income[wage_income["MONTH"] == month_index]
                        # 将该人码的金额列相加即为对应编码总的工资性收入
                        # sum_income = sum(splite_month_data["MONEY"].apply(float))
                        # prin
                        sum_income = self.moneySum(splite_month_data,wage_index)
                        key = str(month_index).strip().zfill(2) + "月"
                        value = float(sum_income)
                        # value = round(sum_income, 1)
                        # totalSum = totalSum + value
                        totalSum = Decimal(str(totalSum)) + Decimal(str(value))
                        # print("99totalSum==>", totalSum)

                        dict[key] = value
                    # sum_income = sum(wage_income["MONEY"].apply(float))
                    res_wage_index = self.findCate(wage_index)
                    # print(wage_index)
                    if( res_wage_index != '0'):
                        # "类别编码：", wage_index, z
                        # print(self.zhibiao[res_wage_index],"总金额：",sum_income)
                        dict["指标"] = "  " + self.zhibiao[res_wage_index]
                        # info = self.zhibiao[wage_index] + "总金额：" + sum_income + '\n'
                        # self.tz_write(info)
                    else:
                        # print(wage_index,"总金额：",sum_income)
                        dict["指标"] = "  " + wage_index
                dict["合计"] = float(totalSum)
                # print(dict)
                self.insert_to_pd(dict)

    # 生成单户台账
    # 功能：根据每一户的信息，提取他们对应的台账信息
    # 输入：户的A表信息，账页数据表
    # 输出：每一户的台账信息
    def taizhang(self,hu,zy):
        # global zhibiao
        # print(hu)
        year_arr = zy["YEAR"].drop_duplicates()
        # month_arr = zy["MONTH"].drop_duplicates()
    # for year_index in year_arr:
        splite_by_year = zy
        # splite_by_year = zy[zy["YEAR"] == year_index]
        # for month_index in month_arr:
        # splite_by_year = zy[zy["MONTH"] == month_index]
        # print(year_index, "年")
        # , month_index, "月:"
        # 处理工资性收入
        # 编码开头两位是21的表示是工资性收入
        wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^21(.*)', x)]
        # 去除表中重复的编码
        # wage_code = set(wage_code)
        # wage_code = list(wage_code)
        # wage_code.sort()
        wage_code = self.hasParent(wage_code)
        if wage_code  :
            data = {"指标": "工资性收入"}
            self.insert_to_pd(data)
            print("工资性收入：")
            self.deal_wage_income(hu,splite_by_year,wage_code)

        # 编码开头两位是22的表示是经营性收入
        wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^22(.*)', x)]
        # wage_code = set(wage_code)
        # wage_code = list(wage_code)
        # wage_code.sort()
        wage_code = self.hasParent(wage_code)
        if wage_code :
            data = {"指标":"经营性收入"}
            self.insert_to_pd(data)
            print("经营性收入：")
            self.deal_wage_income(hu,splite_by_year,wage_code)

        # 编码开头两位是23的表示是财产性收入
        wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^23(.*)', x)]
        # wage_code = set(wage_code)
        # wage_code = list(wage_code)
        # wage_code.sort()
        wage_code = self.hasParent(wage_code)
        if wage_code  :
            data = {"指标":"财产性收入"}
            self.insert_to_pd(data)
            print("财产性收入：")
            self.deal_wage_income(hu, splite_by_year, wage_code)

        # 编码开头两位是24的表示是转移性收入
        wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^24(.*)', x)]
        # wage_code = set(wage_code)
        # wage_code = list(wage_code)
        # wage_code.sort()
        wage_code = self.hasParent(wage_code)
        if wage_code :
            data = {"指标":"转移性收入"}
            self.insert_to_pd(data)
            print("转移性收入：")
            self.deal_wage_income(hu, splite_by_year, wage_code)

        # 编码开头两位是25的表示是非收入所得
        wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^25(.*)', x)]
        # wage_code = set(wage_code)
        # wage_code = list(wage_code)
        # wage_code.sort()
        wage_code = self.hasParent(wage_code)
        if wage_code  :
            data = {"指标":"非收入所得"}
            self.insert_to_pd(data)
            print("非收入所得：")
            self.deal_wage_income(hu, splite_by_year, wage_code)

        # 编码开头两位是26的表示是借贷性所得
        wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^26(.*)', x)]
        # wage_code = set(wage_code)
        # wage_code = list(wage_code)
        # wage_code.sort()
        wage_code = self.hasParent(wage_code)
        if wage_code  :
            data = {"指标":"借贷性所得"}
            self.insert_to_pd(data)
            print("借贷性所得：")
            self.deal_wage_income(hu, splite_by_year, wage_code)

        # 编码开头两位是53的表示是转移性支出
        wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^53(.*)', x)]
        # wage_code = set(wage_code)
        # wage_code = list(wage_code)
        # wage_code.sort()
        wage_code = self.hasParent(wage_code)
        if wage_code  :
            data = {"指标":"转移性支出"}
            self.insert_to_pd(data)
            print("转移性支出：")
            self.deal_wage_income(hu, splite_by_year, wage_code)

        # 编码开头两位是13的表示是农林牧渔生产经营成本
        wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^13(.*)', x)]
        # wage_code = set(wage_code)
        # wage_code = list(wage_code)
        # wage_code.sort()
        wage_code = self.hasParent(wage_code)
        if wage_code  :
            data = {"指标":"农林牧渔生产经营成本"}
            self.insert_to_pd(data)
            print("农林牧渔生产经营成本：")
            self.deal_wage_income(hu, splite_by_year, wage_code)

        # 编码开头两位是16的表示是自产自用实物消费
        wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^16(.*)', x)]
        # wage_code = set(wage_code)
        # wage_code = list(wage_code)
        # wage_code.sort()
        wage_code = self.hasParent(wage_code)
        if wage_code :
            data = {"指标":"自产自用实物消费"}
            self.insert_to_pd(data)
            print("自产自用实物消费：")
            self.deal_wage_income(hu, splite_by_year, wage_code)

        # 编码开头两位是12的表示是农林牧渔生产经营收入
        wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^12(.*)', x)]
        # wage_code = set(wage_code)
        # wage_code = list(wage_code)
        # wage_code.sort()
        wage_code = self.hasParent(wage_code)
        if wage_code :
            data = {"指标":"农林牧渔生产经营收入"}
            self.insert_to_pd(data)
            print("农林牧渔生产经营收入：")
            self.deal_wage_income(hu, splite_by_year, wage_code)

        # 编码开头两位是3的表示是生活消费支出
        wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^3(.*)', x)]
        # wage_code = set(wage_code)
        # wage_code = list(wage_code)
        # wage_code.sort()
        wage_code = self.hasParent(wage_code)
        # print("生活消费支出",wage_code)
        if wage_code :
            # print("进来")
            data = {"指标":"生活消费支出"}
            self.insert_to_pd(data)
            print("生活消费支出：")
        self.deal_wage_income(hu, splite_by_year, wage_code)

if __name__ == '__main__':
    A_path = "D:/Document/Code/Python/AuditingApp/src/输入文件夹/A310151.18.csv"
    D_path = "D:/Document/Code/Python/AuditingApp/src/输入文件夹/D310151.1806.csv"
    zy_path = "D:\Document\Code\Python\AuditingApp\github\Auditing\输入文件夹\账页310151.20181.20181004.csv"
    zhibiao_path = "D:/Document/Code/Python/AuditingApp/src/process/relation_file/zhibiao.json"
    dt = deal_taizhang()
    TableA = dt.read_csv(A_path)
    TableD = dt.read_csv(zy_path)

    community_code = "310151101012001"
    townName = "城桥镇 西泯沟居委"
    communityName = ""

    result = dt.getCommunity(community_code,townName,TableA,TableD)

    print(result)


    # with open(zhibiao_path, 'r') as f:
    #     zhibiao = json.load(f)
    # TableA = read_file(A_path)
    # TableD = read_file(D_path)
    # spliteFamily(TableA,TableD)


