 # _*_ coding=utf-8
# 台账相关处理函数
# getCommunity：将每一户的数据和所有账页数据传给taizhang函数
# taizhang：按年将数据划分，处理每一年的数据，使用正则匹配对应指标的所有数据传给handle_data
# handle_data：处理传入的某一类别记录的所有

import pandas as pd
import math
import re
import json
import sys,os
import myLogging as mylogger
# from myLogging import myLogging

class deal_taizhang():
    def __init__(self):
        # self.logger = myLogging()
        # open('台账.csv', 'w')
        # self.tz = open('输出台账.txt','w')
        # self.tz_write("指标,金额\n")
        # self.tz_write("第二行\n")
        self.result = pd.DataFrame(columns=['指标',"12月","01月","02月","03月","04月","05月","06月","7月","8月","9月","10月","11月","合计"])

        self.zhibiao = {
              "210111": "\u6708\u5de5\u8d44",
              "331": "  #\u79df\u8d41\u623f\u623f\u79df",
              "220611": "\u4ea4\u901a\u8fd0\u8f93.\u4ed3\u50a8\u548c\u90ae\u653f\u4e1a\u6536\u5165",
              "220711": "\u4f4f\u5bbf\u548c\u9910\u996e\u4e1a\u6536\u5165",
              "220811": "\u623f\u5730\u4ea7\u4e1a\u6536\u5165",
              "250261": "  #\u8c03\u67e5\u8865\u8d34",
              "333305": "  #\u7ba1\u9053\u5929\u7136\u6c14",
              "532211": "\u4e2a\u4eba\u7f34\u7eb3\u7684\u533b\u7597\u4fdd\u9669",
              "2403": "\u653f\u7b56\u6027\u751f\u4ea7\u8865\u8d34",
              "33": "3.\u5c45\u4f4f\u7c7b\u652f\u51fa",
              "166311": "\u81ea\u4ea7\u81ea\u7528\u5c45\u4f4f\u7c7b\u8349\u6d88\u8d39 (\u516c\u62c5)",
              "333308": "  #\u7f50\u88c5\u6db2\u5316\u77f3\u6cb9\u6c14",
              "2407": "\u8d61\u517b\u6536\u5165",
              "221011": "\u5c45\u6c11\u670d\u52a1\u3001\u4fee\u7406\u548c\u5176\u4ed6\u670d\u52a1\u4e1a",
              "311": "  #\u98df\u54c1\u6d88\u8d39\u652f\u51fa",
              "230511": "\u51fa\u79df\u623f\u5c4b\u51c0\u6536\u5165",
              "32": "2.\u8863\u7740\u7c7b\u652f\u51fa",
              "26": "\u501f\u8d37\u6027\u6240\u5f97",
              "333211": "  #\u751f\u6d3b\u7528\u7535",
              "2405": "\u62a5\u9500\u533b\u7597\u8d39",
              "221111": "\u5176\u4ed6\u975e\u519c\u884c\u4e1a\u7ecf\u8425\u6536\u5165",
              "133": "\u755c\u7267\u4e1a\u751f\u4ea7\u6210\u672c",
              "210291": "\u5176\u4ed6\u52b3\u52a8\u6240\u5f97",
              "220411": "\u5efa\u7b51\u4e1a\u6536\u5165",
              "9999": "\u751f\u6d3b\u6d88\u8d39\u652f\u51fa",
              "121": "\u519c\u4e1a\u751f\u4ea7\u7ecf\u8425\u6536\u5165",
              "132": "\u6797\u4e1a\u751f\u4ea7\u6210\u672c",
              "240191": "\u5176\u4ed6\u517b\u8001\u91d1",
              "220911": "\u79df\u8d41\u548c\u5546\u52a1\u670d\u52a1\u4e1a\u6536\u5165",
              "539911": "\u5176\u4ed6\u7ecf\u5e38\u8f6c\u79fb\u652f\u51fa",
              "2406": "\u5916\u51fa\u4ece\u4e1a\u4eba\u5458\u5bc4\u56de\u5e26\u56de\u6536\u5165",
              "122": "\u6797\u4e1a\u751f\u4ea7\u7ecf\u8425\u6536\u5165",
              "131": "\u519c\u4e1a\u751f\u4ea7\u6210\u672c",
              "240131": "\u65b0\u578b\u519c\u6751\u517b\u8001\u4fdd\u9669\u91d1",
              "210221": "\u81ea\u7531\u804c\u4e1a\u52b3\u52a8\u6240\u5f97",
              "36": "6.\u6559\u80b2\u6587\u5316\u5a31\u4e50\u652f\u51fa",
              "16302": "\u81ea\u4ea7\u81ea\u7528\u5bb6\u79bd\u6d88\u8d39 (kg)",
              "240121": "\uff08\u57ce\u9547\uff09\u5c45\u6c11\u793e\u4f1a\u517b\u8001\u4fdd\u9669\u91d1",
              "3": "\u5168\u90e8\u751f\u6d3b\u6d88\u8d39\u652f\u51fa",
              "210121": "\u8865\u53d1\u5de5\u8d44",
              "220511": "\u6279\u53d1\u548c\u96f6\u552e\u4e1a\u6536\u5165",
              "2402": "\u793e\u4f1a\u6551\u52a9\u548c\u8865\u52a9",
              "123": "\u755c\u7267\u4e1a\u751f\u4ea7\u7ecf\u8425\u6536\u5165",
              "31": "1.\u98df\u54c1\u70df\u9152\u6d88\u8d39\u652f\u51fa",
              "124": "\u6e14\u4e1a\u751f\u4ea7\u7ecf\u8425\u6536\u5165",
              "240111": "\u79bb\u9000\u4f11\u4eba\u5458\u517b\u8001\u91d1",
              "532311": "\u4e2a\u4eba\u7f34\u7eb3\u7684\u5931\u4e1a\u4fdd\u9669",
              "333306": "  #\u7ba1\u9053\u7164\u6c14",
              "2408": "\u5176\u4ed6\u7ecf\u5e38\u6027\u8f6c\u79fb\u6536\u5165",
              "532411": "\u4e2a\u4eba\u7f34\u7eb3\u7684\u4f4f\u623f\u516c\u79ef\u91d1",
              "25": "\u975e\u6536\u5165\u6240\u5f97",
              "210131": "\u4e0d\u6309\u6708\u53d1\u653e\u7684\u5de5\u8d44",
              "532111": "\u4e2a\u4eba\u7f34\u7eb3\u7684\u517b\u8001\u4fdd\u9669",
              "230411": "\u8f6c\u8ba9\u627f\u5305\u571f\u5730\u7ecf\u8425\u6743\u79df\u91d1\u51c0\u6536\u5165",
              "16101": "\u81ea\u4ea7\u81ea\u7528\u8c37\u7269\u6d88\u8d39 (kg)",
              "34": "4.\u751f\u6d3b\u7528\u54c1\u53ca\u670d\u52a1\u652f\u51fa",
              "220211": "\u5236\u9020\u4e1a\u6536\u5165",
              "134": "\u6e14\u4e1a\u751f\u4ea7\u6210\u672c",
              "333111": "  #\u751f\u6d3b\u7528\u6c34",
              "333307": "  #\u7ba1\u9053\u6db2\u5316\u77f3\u6cb9\u6c14",
              "38": "8.\u5176\u4ed6\u7528\u54c1\u53ca\u670d\u52a1\u652f\u51fa",
              "37": "7.\u533b\u7597\u4fdd\u5065\u652f\u51fa",
              "16303": "\u81ea\u4ea7\u81ea\u7528\u86cb\u7c7b\u6d88\u8d39 (kg)",
              "35": "5.\u4ea4\u901a\u901a\u4fe1\u7c7b\u652f\u51fa",
              "531": "\u4ea4\u7eb3\u6240\u5f97\u7a0e",
              "16109": "\u81ea\u4ea7\u81ea\u7528\u852c\u83dc\u53ca\u98df\u7528\u83cc\u6d88\u8d39 (kg)",
              "230211": "\u96c6\u4f53\u5206\u914d\u7684\u7ea2\u5229"
        }
        # print(os.getcwd())
        mylogger.logger.debug("deal_taizhang init over")
        # self.tz.write(os.getcwd())
        # zhibiao_path = r'D:\Document\Code\Python\AuditingApp\src\process\relation_file\zhibiao.json'
        # with open(zhibiao_path, 'r') as f:
        #     self.zhibiao = json.load(f)

    def tz_write(self,msg):
        with open('输出台账.txt','a') as f:
            f.write(msg)
    def insert_to_pd(self,data):
        self.result = self.result.append(data, ignore_index=True)

    # 读取csv文件
    # def read_file(self,path):
    #     return pd.read_csv(path, header=0, encoding='gbk')

    # 打开csv文件 返回DataFrame对象
    def read_csv(self, path):
        with open(path, 'r') as f:
            # print("open file")
            mylogger.logger.debug('mainWin>openFile:', path)
            file = pd.read_csv(f, header=0)
        return file

    def getCommunity(self,communityCode,TableA,TableD):
        mylogger.logger.debug("deal_taizhang>function:getCommunity")
        pattern = communityCode + '(.*)'
        communityFamily = [x for x in TableA["SID"] if re.match(pattern, x)]

        communityFamily = set(communityFamily)
        i = 0
        for familySID in communityFamily:
            # 获取相同sid的行即为同一户的成员
            family = TableA[TableA["SID"] == familySID]
            zy_data = TableD[TableD["SID"] == familySID]
            # 按照人码进行排序
            family = family.sort_values(by='A100')
            i += 1
            info = {"指标": family["SID"].values[0]}
            self.insert_to_pd(info)

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
            # print(hu_data)
            # if i==5:
            #     break
            i += 1
            # print("============####============")
            # print("户：",i)
            # print("SID：",hu_data["SID"].values[0])
            self.taizhang(hu_data,zy_data)
        # print(TableA["SID"])
        # for row in TableA.iterrows():
        #     print(row[1])

    # 查找指标代码是否存在与指标代码表中，若不存在，则返回其存在与表中的父类
    def findCate(self,index_code):
        # global zhibiao
        if index_code in self.zhibiao:
            return index_code
        else:
            while index_code != '0' and index_code not in self.zhibiao :
                # print("now zhibiao_code:", index_code)
                index_code = str(int(int(index_code) / 10))
                # if  or index_code == '0':
                #     break
        return index_code

    # 判断编码是否有更上层的类别
    def hasParent(self,index_code):
        # 当指标代码不存在与指标表中时，则说明其有所属父类
        while index_code not in self.zhibiao:
            index_code = str(int(int(index_code) / 10))

    def splite_month_data(self,zy,):
        pass

    # 处理每户的账页数据
    def deal_wage_income(self,hu,zy,wage_code):
        # 将账页表中的所有工资性收入数据取出进行处理
        for wage_index in wage_code:
            month_arr = zy["MONTH"].drop_duplicates()
            # 存放每一行将要添加的数据
            dict = {}

            # 该户人家下，该编码对应的所有数据
            wage_income = zy[zy["CODE"] == wage_index]
            # wage_income = splite_month_data[splite_month_data["CODE"] == wage_index]
            # 删除重复人码
            person_code = wage_income["PERSON"].drop_duplicates()

            # 将收支情况按人码区分开来
            # 99表示公共开支，有序号的表示该人对应的收支
            for person_index in person_code:
                # 人码编号<99表示该项收支是某个具体成员的
                if(int(person_index) < 99):
                    # print("<99")
                    # 取对应人码取所有条数的数据
                    person_income = wage_income[wage_income["PERSON"] == person_index]
                    # 将数据按月划分，计算总和添加到dict中
                    for month_index in month_arr:
                        splite_month_data = person_income[person_income["MONTH"] == month_index]
                        # 将该人码的金额列相加即为对应编码总的工资性收入
                        person_wage = sum(splite_month_data["MONEY"].apply(float))
                        key = str(month_index) + "月"
                        value = round(person_wage, 1)
                        dict[key] = value
                    # person_wage = sum(person_income["MONEY"].apply(float))
                    # print("A100:")
                    # print(type(hu.iloc[0,6]))
                    # print("person_index:",person_index,type(person_index))

                    # 取该人码对应成员的姓名
                    person_name = hu[hu["A100"] == int(person_index)]["A101"].values[0]

                    wage_index = self.findCate(wage_index)
                    # 判断指标编码是否在表中存在
                    if( wage_index != '0'):
                        # "类别编码：", wage_index,
                        # print(self.zhibiao[wage_index],"[第",person_index,"人_",person_name, "]:",person_wage)
                        key = "指标"
                        value = self.zhibiao[wage_index] + "[第" + str(person_index) + "人_" + person_name + "]:"
                        # self.tz_write(info)
                        dict[key] = value

                        print("dict",dict)

                    # 若编码在指标列表中不存在，则直接将编码存入
                    else:
                        print(wage_index,"[第",person_index,"人_",person_name, "]:",person_wage)
                        key = "指标"
                        value = str(wage_index) + "[第" + str(person_index) + "人_" + person_name +  "]"
                        dict[key] = value
                        # key = str(month_index) + "月"
                        # value = round(person_wage, 1)
                        # dict[key] = value
                        print("dict", dict)
                    # print("info:",info)
                    # tz.write(info)
                    #     self.tz_write(info)
                # 人码编码为99，表示家庭公共开支，不区分具体人
                else:
                    print('else')
                    # 将该指标的开支按月份分开处理，添加到dict
                    for month_index in month_arr:
                        splite_month_data = wage_income[wage_income["MONTH"] == month_index]
                        # 将该人码的金额列相加即为对应编码总的工资性收入
                        sum_income = sum(splite_month_data["MONEY"].apply(float))
                        key = str(month_index) + "月"
                        value = round(sum_income, 1)
                        dict[key] = value
                    # sum_income = sum(wage_income["MONEY"].apply(float))
                    wage_index = self.findCate(wage_index)
                    # print(wage_index)
                    if( wage_index != '0'):
                        # "类别编码：", wage_index, z
                        print(self.zhibiao[wage_index],"总金额：",sum_income)
                        dict["指标"] = self.zhibiao[wage_index]
                        # info = self.zhibiao[wage_index] + "总金额：" + sum_income + '\n'
                        # self.tz_write(info)
                    else:
                        print(wage_index,"总金额：",sum_income)
                        dict["指标"] = wage_index
                        # info = wage_index + "总金额：" + sum_income + '\n'
                    # info = self.zhibiao[wage_index] + "," + str(sum_income) + "\n"
            self.insert_to_pd(dict)

    # 处理每户的工资性收入
    def before_deal_wage_income(self,hu,zy,wage_code):
        # # 编码开头两位是21的表示是工资性收入
        # wage_code = [x for x in zy['CODE'] if re.match(r'^21(.*)', x)]
        # # 去除表中重复的编码
        # wage_code = set(wage_code)
        # print(wage_code)
        # 将账页表中的所有工资性收入数据取出进行处理
        for wage_index in wage_code:
            wage_income = zy[zy["CODE"] == wage_index]
            # 删除重复人码
            person_code = wage_income["PERSON"].drop_duplicates()
            # print("工资性收入：")
            # print("code:" ,wage_index)
            # 将工资性收入按个人区分开来
            for person_index in person_code:
                # 人码编号99为不区分对应人
                if(int(person_index) < 99):
                    # 取对应人码取所有条数的数据
                    person_income = wage_income[wage_income["PERSON"] == person_index]
                    # 将该人码的金额列相加即为对应编码总的工资性收入
                    person_wage = sum(person_income["MONEY"].apply(float))
                    # print("A100:")
                    # print(type(hu.iloc[0,6]))
                    # print("person_index:",person_index,type(person_index))
                    person_name = hu[hu["A100"] == int(person_index)]["A101"].values[0]
                    print("类别编码：",wage_index,"[第",person_index,"人_",person_name, "]:",person_wage)
                else:
                    sum_income = sum(wage_income["MONEY"].apply(float))
                    print("类别编码：",wage_index,"总金额：",sum_income)

    # 处理经营性收入数据
    def deal_business_income(self,hu,zy):
        business_code = [x for x in zy['CODE'] if re.match(r'^22(.*)', x)]
        business_code = set(business_code)
        print(business_code)

        for business_index in business_code:
            business_income = zy[zy["CODE"] == business_index]

    # 生成单户台账
    # 功能：根据每一户的信息，提取他们对应的台账信息
    # 输入：户的A表信息，账页数据表
    # 输出：每一户的台账信息
    def taizhang(self,hu,zy):
        # global zhibiao
        # print(hu)
        year_arr = zy["YEAR"].drop_duplicates()
        # month_arr = zy["MONTH"].drop_duplicates()
        for year_index in year_arr:
            splite_by_year = zy[zy["YEAR"] == year_index]
            # for month_index in month_arr:
            # splite_by_year = zy[zy["MONTH"] == month_index]
            print(year_index, "年")
            # , month_index, "月:"
            # 处理工资性收入
            # 编码开头两位是21的表示是工资性收入
            wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^21(.*)', x)]
            # 去除表中重复的编码
            wage_code = set(wage_code)
            wage_code = list(wage_code)
            wage_code.sort()
            if wage_code  :
                data = {"指标": "工资性收入"}
                self.insert_to_pd(data)
                print("工资性收入：")
                self.deal_wage_income(hu,splite_by_year,wage_code)

            # 编码开头两位是22的表示是经营性收入
            wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^22(.*)', x)]
            wage_code = set(wage_code)
            wage_code = list(wage_code)
            wage_code.sort()
            if wage_code :
                data = {"指标":"经营性收入"}
                self.insert_to_pd(data)
                print("经营性收入：")
                self.deal_wage_income(hu,splite_by_year,wage_code)

            # 编码开头两位是23的表示是财产性收入
            wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^23(.*)', x)]
            wage_code = set(wage_code)
            wage_code = list(wage_code)
            wage_code.sort()
            if wage_code  :
                data = {"指标":"财产性收入"}
                self.insert_to_pd(data)
                print("财产性收入：")
                self.deal_wage_income(hu, splite_by_year, wage_code)

            # 编码开头两位是24的表示是转移性收入
            wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^24(.*)', x)]
            wage_code = set(wage_code)
            wage_code = list(wage_code)
            wage_code.sort()
            if wage_code :
                data = {"指标":"转移性收入"}
                self.insert_to_pd(data)
                print("转移性收入：",wage_code)
                self.deal_wage_income(hu, splite_by_year, wage_code)

            # 编码开头两位是25的表示是非收入所得
            wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^25(.*)', x)]
            wage_code = set(wage_code)
            wage_code = list(wage_code)
            wage_code.sort()
            if wage_code  :
                data = {"指标":"非收入所得"}
                self.insert_to_pd(data)
                print("非收入所得：")
                self.deal_wage_income(hu, splite_by_year, wage_code)

            # 编码开头两位是26的表示是借贷性所得
            wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^26(.*)', x)]
            wage_code = set(wage_code)
            wage_code = list(wage_code)
            wage_code.sort()
            if wage_code  :
                data = {"指标":"借贷性所得"}
                self.insert_to_pd(data)
                print("借贷性所得：")
                self.deal_wage_income(hu, splite_by_year, wage_code)

            # 编码开头两位是53的表示是转移性支出
            wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^53(.*)', x)]
            wage_code = set(wage_code)
            wage_code = list(wage_code)
            wage_code.sort()
            if wage_code  :
                data = {"指标":"转移性支出"}
                self.insert_to_pd(data)
                print("转移性支出：")
                self.deal_wage_income(hu, splite_by_year, wage_code)

            # 编码开头两位是13的表示是农林牧渔生产经营成本
            wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^13(.*)', x)]
            wage_code = set(wage_code)
            wage_code = list(wage_code)
            wage_code.sort()
            if wage_code  :
                data = {"指标":"农林牧渔生产经营成本"}
                self.insert_to_pd(data)
                print("农林牧渔生产经营成本：")
                self.deal_wage_income(hu, splite_by_year, wage_code)

            # 编码开头两位是16的表示是自产自用实物消费
            wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^16(.*)', x)]
            wage_code = set(wage_code)
            wage_code = list(wage_code)
            wage_code.sort()
            if wage_code :
                data = {"指标":"自产自用实物消费"}
                self.insert_to_pd(data)
                print("自产自用实物消费：")
                self.deal_wage_income(hu, splite_by_year, wage_code)

            # 编码开头两位是12的表示是农林牧渔生产经营收入
            wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^12(.*)', x)]
            wage_code = set(wage_code)
            wage_code = list(wage_code)
            wage_code.sort()
            if wage_code :
                data = {"指标":"农林牧渔生产经营收入"}
                self.insert_to_pd(data)
                print("农林牧渔生产经营收入：")
                self.deal_wage_income(hu, splite_by_year, wage_code)

            # 编码开头两位是3的表示是生活消费支出
            wage_code = [x for x in splite_by_year['CODE'] if re.match(r'^3(.*)', x)]
            wage_code = set(wage_code)
            wage_code = list(wage_code)
            wage_code.sort()
            if wage_code :
                data = {"指标":"生活消费支出"}
                self.insert_to_pd(data)
                print("生活消费支出：")
                self.deal_wage_income(hu, splite_by_year, wage_code)

if __name__ == '__main__':
    A_path = "D:/Document/Code/Python/AuditingApp/src/输入文件夹/A310151.18.csv"
    D_path = "D:/Document/Code/Python/AuditingApp/src/输入文件夹/D310151.1806.csv"
    zhibiao_path = "D:/Document/Code/Python/AuditingApp/src/process/relation_file/zhibiao.json"
    with open(zhibiao_path, 'r') as f:
        zhibiao = json.load(f)
    TableA = read_file(A_path)
    TableD = read_file(D_path)
    spliteFamily(TableA,TableD)


