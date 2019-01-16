# coding:utf-8

import pandas as pd
import re
import datetime
import json
from decimal import Decimal
import myLogging as mylogger

zy_necessity_result = pd.DataFrame()
dict = {}

def insert_to_pd(data):
    global zy_necessity_result
    zy_necessity_result = zy_necessity_result.append(data, ignore_index=True)


def spliteFamily(table):
    sid_array = table["SID"].drop_duplicates()
    for sid_index in sid_array:
        # 获取相同sid的行即为同一户的成员
        hu_data = table[table["SID"] == sid_index]
        # 按照人码进行排序
        hu_data = hu_data.sort_values(by='CODE')
        yield hu_data


def Table(table,code):
     t = table[code]
     # print("tabledata:",t)
     if t.empty == False:
         if pd.isnull(t.values[0]) == True:
             return 0
         if type(t.values[0]) == type("str"):
             # print("字符串类型：",type(t.values[0]))
             return int(t.values[0])
         return t.values[0]
     else:
         return 0


# 获取某个编码的数量 AMOUNT 1
def amountSum(zy,code,person=0):
    # if person != 0:
    #     zy = zy[zy["PERSON"] == person]
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
            return sum(result["AMOUNT"].apply(Decimal))
    # print("无此记录")
    return 0


# 获取某个编码的金额 2
def moneySum(zy,search_code):
    pattern = search_code + "(.*)"
    code = [x for x in zy["CODE"] if re.match(pattern, x)]
    # if search_code == "240111":
    #     print(zy[zy["CODE"] == "240111"])
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
            return sum(result["MONEY"].apply(Decimal))
    # print("无此记录")
    return 0


# 获取某个编码的数量 NOTE  3
def noteSum(zy,code,person=0):
    # if person != 0:
    #     zy = zy[zy["PERSON"] == person]
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
            # print(result["CODE"],result["NOTE"])
            return sum(result["NOTE"].apply(Decimal))
    # print("无此记录")
    return 0


# 单产审核函数
def yield_check(family_zy,code,top,top_tip,low,low_tip,dict):
    amount = amountSum(family_zy, code)
    if amount > 0:
        note = noteSum(family_zy, code)
        # dict['code'] = code + '=' + str(note / amount)
        # print("单产：",note/amount)
        if note / amount > top:
            dict['code'] = "code={},note={],amount={},top={]".format(code,note,amount,top)
            dict['核实内容'] = top_tip
            insert_to_pd(dict)
        if note / amount < low:
            dict['code'] = "code={},note={],amount={},low={]".format(code, note, amount, low)
            dict['核实内容'] = low_tip
            insert_to_pd(dict)


# 出售、购入单价审核函数
def unit_price_check(family_zy,code,top,top_tip,low,low_tip,dict):
    amount = amountSum(family_zy, code)
    # global dict
    if amount > 0:
        money = moneySum(family_zy, code)
        # dict['code'] = code + '=' + str(money/amount)
        # print("单价：",note/amount)
        if money / amount > top:
            dict['code'] = "code={},money={},amount={},money/amount={},top={}".format(code, money, amount, money / amount,top)
            dict['核实内容'] = top_tip
            insert_to_pd(dict)
        if money / amount < low:
            dict['code'] = "code={},money={},amount={},money/amount={},low={}".format(code, money, amount,money / amount, low)
            dict['核实内容'] = low_tip
            # print(low_tip)
            insert_to_pd(dict)


# 农业生产审核
def farm_check(family_zy,code,low,low_tip,dict):
    note = noteSum(family_zy, code)
    if note > 0:
        amount = amountSum(family_zy, code)
        if amount < low:
            # dict['code'] = code + '=' + str(amount)
            dict['code'] = "code={},note={},amount={},low={}".format(code,note,amount,low)
            dict['核实内容'] = low_tip
            insert_to_pd(dict)


# 审核收支及其他
def income_check(family_zy,code,top,top_tip,dict,lenM=1):
    # print("收支：",moneySum(family_zy,code),(lenM * top))
    # print(family_zy)
    money = moneySum(family_zy,code)
    if money > (lenM * top):
        # dict['code'] = code + '=' + str(money)
        dict['code'] = "code={},money={},lenM={},top={}".format(code,money,lenM,top)
        dict['核实内容'] = top_tip
        insert_to_pd(dict)
        # print(top_tip)


# 账页网购审核
def online_shopping_check(family_zy,code,tip,dict):
    note = noteSum(family_zy,code)
    if note != 0:
        dict['code'] = "code={},note={}".format(code,note)
        dict['核实内容'] = tip
        insert_to_pd(dict)
        # print(tip)


# 自产自用审核
def self_use_check(family_zy,code,top,top_tip,dict,lenM=1):
    amount = amountSum(family_zy,code)
    if amount > (lenM * top):
        dict['code'] = "code={},amount={},lenM={},top={}".format(code,amount,lenM,top)
        dict['核实内容'] = top_tip
        insert_to_pd(dict)
        # print(top_tip)


# A表，B表，账页表，住宅表，住户表,小区表
def zy_check_necessity(TableA,TableB,zy,zhuzhai,zhuhu,xiaoqu,result):
    mylogger.logger.debug("zy_necessity init...")
    begin = datetime.datetime.now()
    global zy_necessity_result
    zy_necessity_result.drop(zy_necessity_result.index,inplace=False)
    zy_necessity_result = result
    zy = zy.drop(0)
    zhuzhai = zhuzhai.drop(0)
    zhuhu = zhuhu.drop(0)
    zy = zy.sort_values(by="SID")
    zy_familys = spliteFamily(zy)

    zy["PERSON"] = zy["PERSON"].apply(Decimal)
    zy["NOTE"] = zy["NOTE"].apply(Decimal)
    zy["CODE"] = zy["CODE"].apply(str)
    zy["CODE"] = zy["CODE"].apply(lambda x: x.strip())
    pd.set_option('display.max_columns', None)
    # print(zy[zy["CODE"] == "240111"])

    TableA["A103"] = TableA["A103"].apply(Decimal)
    xiaoqu["VID"] = xiaoqu["VID"].apply(str)
    xiaoqu["VID"] = xiaoqu["VID"].apply(lambda x: x.strip())

    # TableB['B215'] = TableB['B215'].fillna(0)

    # conditions = open("./condition.json")
    with open("./condition.json", 'r', encoding='utf-8-sig') as f:
        conditions = json.load(f)
        # if conditions.startswith(u'\ufeff'):
        #     conditions = conditions.encode('utf8')[3:].decode('utf8')
    yield_condition = conditions["yield"]
    unit_price_condition = conditions["unitPrice"]
    farm_condition = conditions["farm"]
    income_condition = conditions["income"]
    online_condition = conditions["online"]
    self_condition = conditions["self"]

    for zy_family in zy_familys:
        # print(zy_family)
        family_sid = zy_family["SID"].values[0]
        HID = family_sid[0:19]
        A = TableA[TableA["SID"] == family_sid]
        one_zhuhu = zhuhu[zhuhu["HHID"] == family_sid]
        one_zhuzhai = zhuzhai[zhuzhai["HID"] == HID]
        B = TableB[TableB["SID"] == family_sid].copy()
        # B['B215'] = B['B215'].fillna(0)
        qu_vid = str(family_sid[0:15])
        qu = xiaoqu[xiaoqu['VID'] == qu_vid]
        townname = qu['TOWNNAME'].values[0]
        vname = qu['VNAME'].values[0]
        # qu = xiaoqu[xiaoqu['vID'] == qu_vid]
        # townname = qu['townName'].values[0]
        # vname = qu['vName'].values[0]
        ChangeH = 0
        lenM = 0
        surveyType = Table(one_zhuhu, "SURVEYTYPE")
        TaskCode = B[B["SID"] == family_sid]["TASK"].values[0]
        scode = zy_family['SCODE'].values[0]
        # print(TaskCode)
        # yearss = Table(zy_family,"YEAR")
        # print(type(yearss))
        # Year = Table(zy_family,"YEAR")      #int(zy_family["YEAR"].values[0])
        # Month = Table(zy_family,"MONTH")     #int(zy_family["MONTH"].values[0])

        # 账页可能是跨年、多个月份的数据，所以要选择去最小年份的最小月份来算
        Year = min(list(set(zy_family["YEAR"])))
        Year_arr = zy_family[zy_family["YEAR"] == Year]
        Month = min(list(set(Year_arr["MONTH"])))

        dict = {'year': str(Table(zy_family, 'YEAR')), 'month': str(Table(zy_family, 'MONTH')), 'task': str(TaskCode),\
                'scode': scode, 'sid': family_sid,'person':str(99),'townname':townname,'vname':vname}

        hu_zhu = ''
        openYear = Table(one_zhuhu,"OPENYEAR")
        if pd.isnull(openYear) == False:
            openMonth = Table(one_zhuhu,"OPENMONTH")
        # 计算该用户是否报告期内新进住宅
        if TaskCode <= 4:
            lenM = 3
            if openYear == Year and openMonth <= Month and openMonth > Month -2:
                ChangeH = 1
        else:
            # lenM = Month + 1
            lenM = 0
            year_set = list(set(zy_family["YEAR"]))
            # print(year_set)
            for year in year_set:
                month_set = zy_family[zy_family["YEAR"] == year]["MONTH"]
                lenM += len(list(set(month_set)))
            if openYear == Year and openMonth <= Month:
                ChangeH = 1
        # print("month:",lenM)
        # print(one_zhuzhai)
        # a = one_zhuzhai["M105"].values[0]
        # print("a:",a)
        # and one_zhuzhai["M105"].values[0] == 1
        # A["A102"].values[0]
        if (TaskCode >= 1 and TaskCode <= 7) and Table(A,"A102") != 3 and surveyType != 2 and Table(one_zhuzhai,"M105") == 1:
            # 审核家庭中个人帐
            for row in A.iterrows():
                A_member = row[1]
                person = int(A_member["A100"])
                # 取户主姓名(家庭帐审核结果中的name)
                if person == 1:
                    hu_zhu = A_member['A101']
                if person > 0:
                    # 在此统一将person所属账页数据取出，可提高效率
                    person_zy = zy_family[zy_family["PERSON"] == person]
                    # print(person,person_zy)
                    dict['person'] = str(person)
                    dict['name'] = A_member['A101']
                    # if ChangeH == 0:
                    data1 = moneySum(person_zy, "210111")
                    data2 = moneySum(person_zy, "22")
                    a204 = A_member["A204"]
                    if ChangeH == 0 and a204 == 2 and data1 + data2 > 0:
                        dict['code'] = "'210111'={},'22'={},A204={}".format(data1, data2, a204)
                        dict['核实内容'] = "本期内没有就业（a204），但是有按月发放工资或非农生产经营收入(a204~a208)"
                        insert_to_pd(dict)

                    data3 = moneySum(person_zy, "21")
                    if ChangeH == 0 and data1 + data2 > 0 and a204 < 1:
                        dict['code'] = "'21'={},'22'={},A204={}".format(data3, data2, a204)
                        dict['核实内容'] = "有工资、经营收入，未填写从业情况"
                        insert_to_pd(dict)

                    if lenM == 3 and ChangeH == 0:
                        a119 = A_member["A119"]
                        if a119 == 2 and data3 > 0:
                            dict['code'] = "lenM={},ChangeH={},'21'={},,A119={}".format(lenM, ChangeH, data3, a119)
                            dict['核实内容'] = "非常住人员的工资性收入应编入转移类"
                            insert_to_pd(dict)

                        data4 = moneySum(person_zy, "2406")
                        if data1 > 1000 and data4 > 5000:
                            dict['code'] = "'210111'={},'2406'={}".format(data1, data4)
                            dict['核实内容'] = "既有每月发放的工资又有大额的带回收入"
                            insert_to_pd(dict)

                        # 参数 ((A205>C1 && A205<C6)||M212==C0)
                        if (1 < A_member["A205"] < 6 or Table(one_zhuhu, "M212") == 0) and data2 > 0:
                            dict['code'] = "'22'={},A205={}".format(data2, A_member['A205'])
                            dict['核实内容'] = "就业状况 非自营，有经营性收入"
                            insert_to_pd(dict)

                        m1 = moneySum(person_zy, "220111")
                        m2 = moneySum(person_zy, "220211")
                        m3 = moneySum(person_zy, "220311")
                        m4 = moneySum(person_zy, "220411")
                        m5 = moneySum(person_zy, "220511")
                        m6 = moneySum(person_zy, "220611")
                        m7 = moneySum(person_zy, "220711")
                        if ((m1 > 0) + (m2 > 0) + (m3 > 0) + (m4 > 0) + (m5 > 0) + (m6 > 0) + (m7 > 0)) > 2:
                            dict['code'] = "'220111'={},'220211'={},'220311'={},'220411'={},'220511'={},'220611'={},'220711'={}".format(m1, m2, m3, m4, m5, m6, m7)
                            dict['核实内容'] = "收入来源行业有3个或以上(不含房地产业）"
                            insert_to_pd(dict)

                        if a119 == 1 and data4 > 0:
                            dict['code'] = "'2406'={},A119={}".format(data4, a119)
                            dict['核实内容'] = "是常住人口，不应该有寄带回收入。"
                            insert_to_pd(dict)

                    m1 = moneySum(person_zy, "220511")
                    m2 = moneySum(person_zy, "220711")
                    m3 = moneySum(person_zy, "51")
                    m4 = moneySum(person_zy, "5105")
                    m5 = moneySum(person_zy, "5107")
                    if ((data2 - m1 - m2) > 0) and (m3 - m4 - m5 == 0):
                        dict['code'] = "'22'={},'220511'={},'220711'={},'51'={},'5105'={},'5107'={}".format(data2, m1,m2, m3, m4,m5)
                        dict['核实内容'] = "二三产有收入没有成本。已经扣除了批零贸易餐饮业等"
                        insert_to_pd(dict)
                    # print(family_sid,data2,m1,m2,m3,m4,m5)
                    m1 = moneySum(person_zy, "2201")
                    m2 = moneySum(person_zy, "2202")
                    m3 = moneySum(person_zy, "2203")
                    m4 = moneySum(person_zy, "2204")
                    m5 = moneySum(person_zy, "2206")
                    if 6 > A_member["A206"] > 1 and A_member["A205"] == 7 and (m1 + m2 + m3 + m4 + m5) == 0:
                        dict['code'] = "'2201'={},'2202'={},'2203'={},'2204'={},'2206'={},A205={},A206={}".format(m1,m2,m3,m4,m5,A_member['A205'],A_member['A206'])
                        dict['核实内容'] = "从事第二产业经营的就业人员无第二产业收入"
                        insert_to_pd(dict)

                    m1 = moneySum(person_zy, "2401")
                    m2 = moneySum(person_zy, "2101")
                    a201 = A_member['A201']
                    if a201 == 1 and m1 == 0 and m2 > 0:
                        dict['code'] = "'2401'={},'2101'={},A201={}".format(m1, m2, a201)
                        dict['核实内容'] = "行政事业单位离退休人员，没有养老金收入,但是有工资收入"
                        insert_to_pd(dict)

                    if a201 == 2 and m1 == 0 and m2 > 0:
                        dict['code'] = "'2401'={},'2101'={},A201={}".format(m1, m2, a201)
                        dict['核实内容'] = "其他单位离退休人员，没有养老金收入,但是有工资收入"
                        insert_to_pd(dict)

            dict['name'] = hu_zhu
            data = moneySum(zy_family,"230511")
            if ChangeH == 0 and data > 0 and Table(B,"B128") + Table(B,"B131") < 0.1:
                dict['code'] = "ChangeH={},'230511'={},B128={},B131={},B128+B131<0.1".format(ChangeH,data1,Table(B,"B128"),Table(B,"B131"))
                dict['核实内容'] = "有租金收入，但b表没有房屋出租信息(b128,b131)"
                insert_to_pd(dict)
            data1 = moneySum(zy_family,"12")
            data2 = moneySum(zy_family,"13")
            if ChangeH == 0 and data1 > 0 and data2 == 0:
                dict['code'] = "ChangeH={},'12'={},'13'={}".format(ChangeH,data1,data2)
                dict['核实内容'] = "一产有收入，没有成本"
                insert_to_pd(dict)

            code_arr = ["551311","551321","551331","551341","144111","144211","144311","144911"]
            for i in code_arr:
                if 0 < moneySum(zy_family,i) < 1000:
                    dict['核实内容'] = "小于1000元不计入固定资产，计入经营费用中"
                    insert_to_pd(dict)

            # 消费相关
            if surveyType == 1:
                if ChangeH == 0:
                    data1 = moneySum(zy_family,"351311")
                    data2 = moneySum(zy_family,"351321")
                    if data1 + data2 > 200 and Table(B,"B201") + Table(B,"B202") == 0:
                        dict['code'] = "ChangeH={},'351311'={},'351321'={},B201={},B202={}".format(ChangeH,data1,data2,Table(B,"B201"),Table(B,"B202"))
                        dict['核实内容'] = "有汽柴油支出,没有汽车或摩托车（b201，b202）"
                        insert_to_pd(dict)
                    data = moneySum(zy_family,"352221")
                    if data > 0 and Table(B,"B216") < 1:
                        dict['code'] = "ChangeH={},'352221'={},B216={}".format(ChangeH,data,Table(B, "B216"))
                        dict['核实内容'] = "有移动电话费支出，但没有移动电话？（b216）"
                        insert_to_pd(dict)

                    data = moneySum(zy_family,"352211")
                    if data > 0 and Table(B,"B215") == 0:
                        dict['code'] = "ChangeH={},'352211'={},B215={}".format(ChangeH,data, Table(B, "B215"))
                        dict['核实内容'] = "有固定电话费支出，但没有固定电话（b215）"
                        insert_to_pd(dict)
                    # print(family_sid,surveyType,Table(A,"A102"),Table(one_zhuzhai,"M105"),ChangeH,data,Table(B,"B215"))

                    data = moneySum(zy_family,"352231")
                    if (Table(B,"B219") + Table(B,"B217")) == 0 and data > 90:
                        dict['code'] = "ChangeH={},'352231'={},B217={},B219={}".format(ChangeH,data, Table(B, "B217"),Table(B,"B219"))
                        dict['核实内容'] = "上网费用支出大于90，但没有接入互联网的计算机或手机？（b219，b217）"
                        insert_to_pd(dict)

                    data = moneySum(zy_family,"362351")
                    if data > 0 and Table(B,"B208") == 0:
                        dict['code'] = "ChangeH={},'362351'={},B208={}".format(ChangeH,data, Table(B, "B208"))
                        dict['核实内容'] = "有有线电视费支出，但没有接入有线电视网的彩色电视机（b208）"
                        insert_to_pd(dict)

                    data1 = moneySum(zy_family,"240511")
                    data2 = moneySum(zy_family,"371111")
                    data3 = moneySum(zy_family,"372")
                    if data1 > 0 and data2 + data3 == 0:
                        dict['code'] = "ChangeH={},'240511'={},'3711111'={},'372'={}".format(ChangeH,data1,data2,data3)
                        dict['核实内容'] = "有报销医疗费，但无药品或医疗服务支出"
                        insert_to_pd(dict)

                data1 = moneySum(zy_family,"3513")
                data2 = moneySum(zy_family,"3514")
                if Table(B,"B201") + Table(B,"B202") > 0 and data1 + data2 == 0:
                    dict['code'] = "'3513'={},'3514'={},B201={},B202={}".format(data1,data2,Table(B,"B201"),Table(B, "B202"))
                    dict['核实内容'] = "有汽车或摩托车，却没有交通用燃料支出和使用维修支出"
                    insert_to_pd(dict)

                data = moneySum(zy_family,"333")
                if data == 0:
                    dict['code'] = "'333'={}".format(data)
                    dict['核实内容'] = "水电燃料支出为0"
                    insert_to_pd(dict)

                data1 = moneySum(zy_family,"311")
                data2 = moneySum(zy_family,"21")
                data3 = moneySum(zy_family,"22")
                if data1 < 100 and data2 + data3 > 10000:
                    dict['code'] = "'311'={},'21'={},'22'={}".format(data1,data2,data3)
                    dict['核实内容'] = "总收入大于10000，食品支出小于100"
                    insert_to_pd(dict)


            # 审核家庭账
            family_zy = zy_family[zy_family["PERSON"] == 99]
            dict['person'] = str(99)
            dict['name'] = hu_zhu

            # 审核单产
            for con in yield_condition:
                yield_check(family_zy,con["code"],con["top"],con["topTip"],con["low"],con["lowTip"],dict)

            # 审核单价(出售/购买)
            for con in unit_price_condition:
                # print(con)
                unit_price_check(family_zy,con["code"],con["top"],con["topTip"],con["low"],con["lowTip"],dict)

            # 农业生产审核
            for con in farm_condition:
                farm_check(family_zy,con["code"],con["low"],con["lowTip"],dict)

            # 收支及其他
            for con in income_condition:
                if con["hasLenM"] == 1:
                    income_check(zy_family,con["code"],con["top"],con["topTip"],dict,lenM)
                else:
                    income_check(zy_family,con["code"],con["top"],con["topTip"],dict)

            # 账页网购审核
            for con in online_condition:
                online_shopping_check(family_zy,con["code"],con["tip"],dict)

            # 自产自用审核
            for con in self_condition:
                if con["hasLenM"] == 1:
                    self_use_check(family_zy,con["code"],con["top"],con["topTip"],dict,lenM)
                else:
                    self_use_check(family_zy,con["code"],con["top"],con["topTip"],dict)

    return zy_necessity_result

    end = datetime.datetime.now()
    print("运行时间：",end - begin)




# 打开csv文件 返回DataFrame对象
def read_csv(path):
    with open(path, 'r') as f:
        df = pd.read_csv(f, header=0,low_memory=False)
        col = colUpper(df.columns.values.tolist())
        df = df.rename(columns=col)
    return df

# 将所有列名换成大写
def colUpper(col):
    dict = {}
    for key in col:
        value = key.upper()
        # print(value)
        dict[key] = value
    return dict

if __name__ == '__main__':
    A_path = r"D:\研一\项目\测试数据\外部程序测试数据\外部程序测试数据\18年A卷数据.csv"
    B_path = r"D:\研一\项目\测试数据\外部程序测试数据\外部程序测试数据\18年B卷数据.csv"
    zy_path = r"D:\研一\项目\测试数据\外部程序测试数据\外部程序测试数据\18年账页数据.csv"
    xiaoqu_path = r"D:\研一\项目\测试数据\外部程序测试数据\外部程序测试数据\S310151.18_1.csv"

    # zy_path = u"D:/Document/Code/Python/AuditingApp/src/输入文件夹/D310151.1806.csv"
    zhuhu_path = u"D:\研一\项目\githubProjects\code\AuditingAPP\输入文件夹\住户样本310151.18.csv"
    zhuzhai_path = u"D:\研一\项目\githubProjects\code\AuditingAPP\输入文件夹\住宅名录310151.18.csv"
    # xiaoqu_path = u"D:\研一\项目\githubProjects\code\AuditingAPP\输入文件夹\小区名录310151.18.csv"
    xiaoqu = read_csv(xiaoqu_path)
    TableA = read_csv(A_path)
    TableB = read_csv(B_path)
    zy = read_csv(zy_path)
    zhuzhai = read_csv(zhuzhai_path)
    zhuhu = read_csv(zhuhu_path)
    head = {'sid': [], 'year': [], 'task': [], 'month': [], 'scode': [], 'person': [], 'name': [], 'code': [],
            '核实内容': [], 'townname': [], 'vname': []}
    result = pd.DataFrame(head)
    result = result[['sid', 'year', 'month', 'task', 'scode', 'person', 'name', 'code', '核实内容', 'townname', 'vname']]  # , 'haddr']]

    zy_check_necessity(TableA,TableB,zy,zhuzhai,zhuhu,xiaoqu,result)
    zy_necessity_result.to_excel('zy_necessity_result.xlsx',encoding="utf-8",index=False,sheet_name='Sheet')







