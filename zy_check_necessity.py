# coding:utf-8

import pandas as pd
import re
import datetime
import json
from decimal import Decimal


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
def moneySum(zy,code,person=0):
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
            # print(result["CODE"],result["MONEY"])
            return sum(result["NOTE"].apply(Decimal))
    # print("无此记录")
    return 0

# 出售单价审核函数
def sale_price_check(family_zy,code,top,top_tip,low,low_tip):
    amount = amountSum(family_zy, code)
    if amount > 0:
        note = noteSum(family_zy, code)
        print("单价：",note/amount)
        if note / amount > top:
            print(top_tip)
        if note / amount < low:
            print(low_tip)
# 农业生产审核
def farm_check(family_zy,code,low,low_tip):
    if noteSum(family_zy, code) > 0:
        if amountSum(family_zy, code) < low:
            print(low_tip)
# 审核收支及其他
def income_check(family_zy,code,top,top_tip,lenM=1):
    # print("收支：",moneySum(family_zy,code),(lenM * top))
    if moneySum(family_zy,code) > (lenM * top):
        print(top_tip)
# 账页网购审核
def online_shopping_check(family_zy,code,tip):
    if noteSum(family_zy,code) != 0:
        print(tip)
# 自产自用审核
def self_use_check(family_zy,code,top,top_tip,lenM=1):
    if amountSum(family_zy,code) > (lenM * top):
        print(top_tip)

# A表，B表，住宅表，住户表
def zy_check_necessity(TableA,TableB,zy,zhuzhai,zhuhu):
    begin = datetime.datetime.now()

    zy = zy.drop(0)
    zhuzhai = zhuzhai.drop(0)
    zhuhu = zhuhu.drop(0)
    zy = zy.sort_values(by="SID")
    zy_familys = spliteFamily(zy)

    # conditions = open("./condition.json")
    with open("./输入文件夹/condition.json", 'r', encoding='UTF-8') as f:
        conditions = json.load(f)
    sale_condition = conditions["salePrice"]
    farm_condition = conditions["farm"]
    income_condition = conditions["income"]
    online_condition = conditions["online"]
    self_condition = conditions["self"]

    for zy_family in zy_familys:
        # print(zy_family)
        family_sid = zy_family["SID"].values[0]
        HID = family_sid[0:19]
        # print(HID)
        # print(family_sid)
        A = TableA[TableA["SID"] == family_sid]
        one_zhuhu = zhuhu[zhuhu["HHID"] == family_sid]
        one_zhuzhai = zhuzhai[zhuzhai["HID"] == HID]
        B = TableB[TableB["SID"] == family_sid]
        ChangeH = 0
        lenM = 0
        surveyType = int(one_zhuhu["SURVEYTYPE"].values[0])
        # print(surveyType)
        TaskCode = B[B["SID"] == family_sid]["TASK"].values[0]
        # print(TaskCode)
        # yearss = Table(zy_family,"YEAR")
        # print(type(yearss))
        # Year = Table(zy_family,"YEAR")      #int(zy_family["YEAR"].values[0])
        # Month = Table(zy_family,"MONTH")     #int(zy_family["MONTH"].values[0])
        # 账页可能是跨年、多个月份的数据，所以要选择去最小年份的最小月份来算
        Year = min(list(set(zy_family["YEAR"])))
        Year_arr = zy_family[zy_family["YEAR"] == Year]
        Month = min(list(set(Year_arr["MONTH"])))
        print(Year,Month)

        openYear = Table(one_zhuhu,"OPENYEAR")  #one_zhuhu["OPENYEAR"].values[0]
        if pd.isnull(openYear) == False:
            # openYear = int(one_zhuhu["OPENYEAR"].values[0])
            openMonth = Table(one_zhuhu,"OPENMONTH")     #int(one_zhuhu["OPENMONTH"].values[0])
        # 计算该用户是否报告期内新进住宅
        if TaskCode <= 4:
            lenM = 3
            if openYear == Year and openMonth <= Month and openMonth > Month -2:
                ChangeH = 1
        else:
            lenM = Month + 1
            if openYear == Year and openMonth <= Month:
                ChangeH = 1
        # print(one_zhuzhai)
        # a = one_zhuzhai["M105"].values[0]
        # print("a:",a)
        # and one_zhuzhai["M105"].values[0] == 1
        # A["A102"].values[0]
        if (TaskCode >= 1 and TaskCode <= 7) and Table(A,"A102") != 3 and surveyType != 2 and Table(one_zhuzhai,"M105") == 1:
            # print(" in 1")
            if ChangeH == 0 and moneySum(zy_family,"230511") > 0 and Table(B,"B128") + Table(B,"B131") < 0.1:# B["B128"].values[0] + B["B131"].values[0] < 0.1:
                print("有租金收入，但b表没有房屋出租信息(b128,b131)")
            if ChangeH == 0 and moneySum(zy_family,"12") > 0 and moneySum(zy_family,"13") == 0:
                print("一产有收入，没有成本")
            code_arr = ["551311","551321","551331","551341","144111","144211","144311","144911"]
            for i in code_arr:
                if 0<moneySum(zy_family,i)<1000:
                    print("小于1000元不计入固定资产，计入经营费用中")

            # print("surveyType:",surveyType,type(surveyType))
            # print(surveyType == 1)
            # 消费相关
            if surveyType == 1:
                # print("in 2")
                if ChangeH == 0:
                    if moneySum(zy_family,"351311") + moneySum(zy_family,"351321") > 200 and Table(B,"B201") + Table(B,"B201") == 0:
                    #B["B201"].values[0] + B["B202"].values[0] < 0:
                        print("有汽柴油支出,没有汽车或摩托车（b201，b202）")
                    if moneySum(zy_family,"352221") > 0 and Table(B,"B216") < 1:
                        print("有移动电话费支出，但没有移动电话？（b216）")
                    if moneySum(zy_family,"352211") > 0 and Table(B,"B215") == 0:
                        print(family_sid,Table(B,"B215"))
                        print("有固定电话费支出，但没有固定电话（b215）")
                    if (Table(B,"B219") + Table(B,"B217")) == 0 and moneySum(zy_family,"352231") > 90:
                        print("上网费用支出大于90，但没有接入互联网的计算机或手机？（b219，b217）")
                    if moneySum(zy_family,"362351") > 0 and Table(B,"B208") == 0:
                        print("有有线电视费支出，但没有接入有线电视网的彩色电视机（b208）")
                    if moneySum(zy_family,"240511") > 0 and moneySum(zy_family,"371111") + moneySum(zy_family,"372") == 0:
                        print(family_sid,"有报销医疗费，但无药品或医疗服务支出")

                if Table(B,"B201") + Table(B,"B202") > 0 and moneySum(zy_family,"3513") + moneySum(zy_family,"3514") == 0:
                    print("有汽车或摩托车，却没有交通用燃料支出和使用维修支出")
                if moneySum(zy_family,"333") == 0:
                    print(family_sid,"水电燃料支出为0 ")
                if moneySum(zy_family,"311") < 100 and (moneySum(zy_family,"21") + moneySum(zy_family,"22")) > 10000:
                    print("总收入大于10000，食品支出小于100")

            # 审核家庭中个人帐
            for row in A.iterrows():
                A_member = row[1]
                # print(A_member["A100"])
                person = int(A_member["A100"])
                if person > 0:
                    # 在此统一将person所属账页数据取出，可提高效率
                    person_zy = zy_family[zy_family["PERSON"] == str(person)]
                    # print(type(zy_family["PERSON"].values[0]),type(person))
                    if ChangeH == 0:
                        if A_member["A204"] == 2 and moneySum(person_zy,"210111",person) + moneySum(person_zy,"22",person) > 0:
                            print("本期内没有就业（a204），但是有按月发放工资或非农生产经营收入(a204~a208)")
                        if moneySum(person_zy,"21",person) + moneySum(person_zy,"22",person) > 0 and A_member["A204"] < 1:
                            print("有工资、经营收入，未填写从业情况")
                        if lenM == 3 and ChangeH == 0:
                            if A_member["A119"] == 2 and moneySum(person_zy,"21",person) > 0:
                                print("非常住人员的工资性收入应编入转移类")
                            if moneySum(person_zy,"210111",person) > 1000 and moneySum(person_zy,"2406",person) > 5000:
                                print("既有每月发放的工资又有大额的带回收入")
                            # 参数 ((A205>C1 && A205<C6)||M212==C0)
                            if (1 < A_member["A205"] < 6 or Table(one_zhuhu,"M212") == 0) and moneySum(person_zy,"22",person) > 0:
                                print("就业状况 非自营，有经营性收入")
                            if ((moneySum(person_zy,"220111",person) > 0) + (moneySum(person_zy,"220211",person) > 0) + (moneySum(person_zy,"220311",person) > 0) + (moneySum(person_zy,"220411",person) > 0) + (moneySum(person_zy,"220511",person) > 0) + (moneySum(person_zy,"220611",person) > 0) + (moneySum(person_zy,"220711",person) > 0)) > 2:
                                print("收入来源行业有3个或以上(不含房地产业）")
                            if A_member["A119"] == 1 and moneySum(person_zy,"2406",person) > 0:
                                print("是常住人口，不应该有寄带回收入。")

                        # print(person,person_zy)
                        if ((moneySum(person_zy,"22",person) - moneySum(person_zy,"220511",person) - moneySum(person_zy,"220711",person)) > 0) and ((moneySum(person_zy,"51",person)) - moneySum(person_zy,"5105",person) - moneySum(person_zy,"5107",person) == 0):
                            print("二三产有收入没有成本。已经扣除了批零贸易餐饮业等")
                        if 6 > A_member["A206"] > 1 and A_member["A205"] == 7 and (moneySum(person_zy,"2201",person) + moneySum(person_zy,"2202",person) + moneySum(person_zy,"2203",person) + moneySum(person_zy,"2204",person) + moneySum(person_zy,"2206",person)) == 0:
                            print("从事第二产业经营的就业人员无第二产业收入")
                        if A_member["A201"] == 1 and moneySum(person_zy,"2401",person) == 0 and moneySum(person_zy,"2101",person) > 0:
                            print("行政事业单位离退休人员，没有养老金收入,但是有工资收入")
                        if A_member["A201"] == 2 and moneySum(person_zy,"2401",person) == 0 and moneySum(person_zy,"2101",person) > 0:
                            print("其他单位离退休人员，没有养老金收入,但是有工资收入")

            # 审核家庭账
            family_zy = zy_family[zy_family["PERSON"] == 99]

            # 审核单产
            amount = amountSum(family_zy, "111011")
            if amount > 0:
                note = noteSum(family_zy,"111011")
                if note / amount > 800:
                    print("小麦单产偏高")
                if note / amount < 50:
                    print("小麦单产偏低")

            amount = amountSum(family_zy, "111012")
            if amount > 0:
                note = noteSum(family_zy, "111012")
                if note / amount > 1050:
                    print("稻谷单产偏高")
                if note / amount < 50:
                    print("稻谷单产偏低")

            amount = amountSum(family_zy, "111013")
            if amount > 0:
                note = noteSum(family_zy, "111013")
                if note / amount > 1300:
                    print("玉米单产偏高")
                if note / amount < 50:
                    print("玉米单产偏低")

            amount = amountSum(family_zy, "111014")
            if amount > 0:
                note = noteSum(family_zy, "111014")
                if note / amount > 600:
                    print("高粱单产偏高")
                if note / amount < 50:
                    print("高粱单产偏高")

            amount = amountSum(family_zy, "111015")
            if amount > 0:
                note = noteSum(family_zy, "111015")
                if note / amount > 1000:
                    print("谷子单产偏高")
                if note / amount < 50:
                    print("谷子单产偏低")

            amount = amountSum(family_zy, "111016")
            if amount > 0:
                note = noteSum(family_zy, "111016")
                if note / amount > 1000:
                    print("青稞单产偏高")
                if note / amount < 10:
                    print("青稞单产偏低")

            amount = amountSum(family_zy, "111021")
            if amount > 0:
                note = noteSum(family_zy, "111021")
                if note / amount > 10000:
                    print("红薯单产偏高")
                if note / amount < 50:
                    print("红薯单产偏低")

            amount = amountSum(family_zy, "111022")
            if amount > 0:
                note = noteSum(family_zy, "111022")
                if note / amount > 10000:
                    print("马铃薯单产偏高")
                if note / amount < 50:
                    print("马铃薯单产偏低")

            amount = amountSum(family_zy, "111029")
            if amount > 0:
                note = noteSum(family_zy, "111029")
                if note / amount > 10000:
                    print("其他薯类单产偏高")
                if note / amount < 50:
                    print("其他薯类单产偏低")

            amount = amountSum(family_zy, "111031")
            if amount > 0:
                note = noteSum(family_zy, "111031")
                if note / amount > 1000:
                    print("大豆单产偏高")
                if note / amount < 30:
                    print("大豆单产偏低")
            amount = amountSum(family_zy, "111039")
            if amount > 0:
                note = noteSum(family_zy, "111039")
                if note / amount > 1000:
                    print("其他豆类单产偏高")
                if note / amount < 30:
                    print("其他豆类单产偏低")
            amount = amountSum(family_zy, "111041")
            if amount > 0:
                note = noteSum(family_zy, "111041")
                if note / amount > 1000:
                    print("棉花单产偏高")
                if note / amount < 20:
                    print("棉花单产偏低")
            amount = amountSum(family_zy, "111051")
            if amount > 0:
                note = noteSum(family_zy, "111051")
                if note / amount > 1000:
                    print("花生单产偏高")
                if note / amount < 20:
                    print("花生单产偏低")
            amount = amountSum(family_zy, "111052")
            if amount > 0:
                note = noteSum(family_zy, "111052")
                if note / amount > 1000:
                    print("芝麻单产偏高")
                if note / amount < 10:
                    print("芝麻单产偏低")
            amount = amountSum(family_zy, "111053")
            if amount > 0:
                note = noteSum(family_zy, "111053")
                if note / amount > 1000:
                    print("油菜籽单产偏高")
                if note / amount < 20:
                    print("油菜籽单产偏低")
            amount = amountSum(family_zy, "111054")
            if amount > 0:
                note = noteSum(family_zy, "111054")
                if note / amount > 1000:
                    print("葵花籽单产偏高")
                if note / amount < 20:
                    print("葵花籽单产偏低")
            amount = amountSum(family_zy, "111059")
            if amount > 0:
                note = noteSum(family_zy, "111059")
                if note / amount > 2000:
                    print("其他油料单产偏高")
                if note / amount < 20:
                    print("其他油料单产偏低")

            # 审核单价(出售/购买)
            for con in sale_condition:
                # print(con)
                sale_price_check(family_zy,con["code"],con["top"],con["topTip"],con["low"],con["lowTip"])

            # 农业生产审核
            for con in farm_condition:
                farm_check(family_zy,con["code"],con["low"],con["lowTip"])

            # 收支及其他
            for con in income_condition:
                if con["hasLenM"] == 1:
                    income_check(family_zy,con["code"],con["top"],con["topTip"],lenM)
                else:
                    income_check(family_zy,con["code"],con["top"],con["topTip"])

            # 账页网购审核
            for con in online_condition:
                online_shopping_check(family_zy,con["code"],con["tip"])

            # 自产自用审核
            for con in self_condition:
                if con["hasLenM"] == 1:
                    self_use_check(family_zy,con["code"],con["top"],con["topTip"],lenM)
                else:
                    self_use_check(family_zy,con["code"],con["top"],con["topTip"])


    end = datetime.datetime.now()
    print("运行时间：",end - begin)




# 打开csv文件 返回DataFrame对象
def read_csv(path):
    with open(path, 'r') as f:
        file = pd.read_csv(f, header=0)
    return file


if __name__ == '__main__':
    A_path = u"D:/Document/Code/Python/AuditingApp/src/输入文件夹/A310151.18.csv"
    B_path = u"D:/Document/Code/Python/AuditingApp/src/输入文件夹/B310151.18.csv"
    zy_path = u"D:\Document\Code\Python\AuditingApp\github\Auditing\输入文件夹\账页310151.20181.20181004.csv"
    # zy_path = u"D:/Document/Code/Python/AuditingApp/src/输入文件夹/D310151.1806.csv"
    zhuhu_path = u"D:/Document/Code/Python/AuditingApp/src/输入文件夹/住户样本310151.18.csv"
    zhuzhai_path = u"D:/Document/Code/Python/AuditingApp/src/输入文件夹/住宅名录310151.18.csv"

    TableA = read_csv(A_path)
    TableB = read_csv(B_path)
    zy = read_csv(zy_path)
    zhuzhai = read_csv(zhuzhai_path)
    zhuhu = read_csv(zhuhu_path)

    zy_check_necessity(TableA,TableB,zy,zhuzhai,zhuhu)







