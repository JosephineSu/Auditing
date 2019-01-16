import pandas as pd
import re
import datetime
from decimal import Decimal
import myLogging as mylogger

zy_addition_result = pd.DataFrame()
dict = {}


def spliteFamily(table):
    sid_array = table["SID"].drop_duplicates()
    for sid_index in sid_array:
        # 获取相同sid的行即为同一户的成员
        hu_data = table[table["SID"] == sid_index]
        # 按照人码进行排序
        hu_data = hu_data.sort_values(by='CODE')
        yield hu_data


def Table(table, code):
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


# 获取某个编码的金额 2
def moneySum(zy,code):
    pattern = code + "(.*)"
    code = [x for x in zy['CODE'] if re.match(pattern, x)]
    if len(code) != 0:
        code = set(code)
        code = list(code)
        code.sort()
        frames = []
        for index in code:
            df = zy[zy['CODE'] == index]
            frames.append(df)
        if len(frames) != 0:
            result = pd.concat(frames)
            return sum(result['MONEY'].apply(Decimal))
            # return Decimal(str(sum(result['MONEY'].apply(Decimal))))
    return 0


def judge_moneySum(x):
    if x > 0:
        return 1
    else:
        return 0


def insert_to_pd(data):
    global zy_addition_result
    zy_addition_result = zy_addition_result.append(data, ignore_index=True)


def zy_check_addition(TableA,TableB,zy,zhuzhai,zhuhu,xiaoqu,result):
    mylogger.logger.debug("zy_check_addition init..")
    begin = datetime.datetime.now()

    global zy_addition_result
    zy_addition_result.drop(zy_addition_result.index,inplace=True)
    zy_addition_result = result
    zy = zy.drop(0)
    zhuzhai = zhuzhai.drop(0)
    zhuhu = zhuhu.drop(0)
    zy = zy.sort_values(by='SID')
    zy_families = spliteFamily(zy)
    zy["PERSON"] = zy["PERSON"].apply(Decimal)
    zy["CODE"].apply(str)
    zy["CODE"] = zy["CODE"].apply(lambda x: x.strip())
    xiaoqu["VID"] = xiaoqu["VID"].apply(str)
    xiaoqu["VID"] = xiaoqu["VID"].apply(lambda x: x.strip())

    for zy_family in zy_families:
        family_sid = zy_family['SID'].values[0]
        A = TableA[TableA['SID'] == family_sid]
        if A.empty == True:
            # print("A表中无此户")
            continue
        HID = family_sid[0:19]
        one_zhuhu = zhuhu[zhuhu['HHID'] == family_sid]
        one_zhuzhai = zhuzhai[zhuzhai['HID'] == HID]
        B = TableB[TableB['SID'] == family_sid]
        qu_vid = family_sid[0:15]
        qu = xiaoqu[xiaoqu['VID'] == qu_vid]
        townname = qu['TOWNNAME'].values[0]
        vname = qu['VNAME'].values[0]
        ChangeH = 0
        LenM = 0
        surveyType = Table(one_zhuhu, "SURVEYTYPE")
        TaskCode = Table(B,'TASK')
        task = TaskCode
        scode = zy_family['SCODE'].values[0]
        # Year = Table(zy_family,'YEAR')
        # Month = Table(zy_family,'MONTH')
        Year = min(list(set(zy_family["YEAR"])))
        Year_arr = zy_family[zy_family["YEAR"] == Year]
        Month = min(list(set(Year_arr["MONTH"])))
        openYear = Table(one_zhuhu,'OPENYEAR')
        dict = {'year': str(Table(zy_family, 'YEAR')), 'month': str(Table(zy_family, 'MONTH')), 'task': str(task),
                'scode': scode, 'sid': family_sid, 'person': str(99),'townname': townname, 'vname': vname}

        if pd.isnull(openYear) == False:
            # openYear = int(one_zhuhu["OPENYEAR"].values[0])
            openMonth = Table(one_zhuhu,"OPENMONTH")     #int(one_zhuhu["OPENMONTH"].values[0])
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

        if (TaskCode >= 1 and TaskCode <= 7)and A['A102'].values[0] != 3 and surveyType == 1:
            # 消费相关,家庭中个人账
            for row in A.iterrows():
                A_member = row[1]
                person = int(A_member['A100'])
                name = A_member['A101']
                if person == 1:
                    huzhu = A_member['A101']
                if person > 0:
                    if person == 99:
                        name = huzhu
                    person_zy = zy_family[zy_family['PERSON'] == person]
                    sum1 = 0
                    sum2 = 0
                    for i in range(2201, 2211):
                        sum1 += judge_moneySum(moneySum(person_zy, str(i)))
                        i += 1
                    for j in range(24011, 24013):
                        sum2 += judge_moneySum(moneySum(person_zy, str(j)))
                        j += 1
                    if ChangeH == 0:
                        dict['name'] = name
                        dict['person'] = str(person)
                        sum = judge_moneySum(moneySum(person_zy, "21"))
                        if sum + sum1 + sum2 >= 3:
                            dict['code'] = "ChangH={},'21'={},'2201~2211'={},'24011~24013'={}".format(ChangeH,sum,sum1,sum2)
                            dict['提示信息'] = "一人有三种及以上收入来源"
                            insert_to_pd(dict)
                        if sum1 >= 2:
                            dict['code'] = "ChangH={},'2201~2211'={}".format(ChangeH,sum1)
                            dict['提示信息'] = "一人有两种及以上经营收入"
                            insert_to_pd(dict)
                        if sum2 >= 2:
                            dict['code'] = "ChangH={},'24011~24013'={}".format(ChangeH, sum2)
                            dict['提示信息'] = "一人有两种及以上养老收入（不含其他商业保险养老情况）"
                            insert_to_pd(dict)
                        if TaskCode <= 4:
                            m1 = moneySum(person_zy, "210111")
                            if A_member['A204'] == 2 and m1 > 0:
                                dict['code'] = "ChangH={},TaskCode={},A204={},'210111'={}".format(ChangeH,TaskCode,A_member['A204'],m1)
                                dict['提示信息'] = "本季度末未就业有按月发放的工资"
                                insert_to_pd(dict)

                            m2 = moneySum(person_zy, "240111")
                            if A_member['A202'] != 1 and A_member['A201'] == 3 and m2 > 0:
                                dict['code'] = "ChangH={},TaskCode={},A202={},A201={},'240111'={}".format(ChangeH, TaskCode,A_member['A202'],A_member['A201'], m2)
                                dict['提示信息'] = "本季度末退休有离退休金"
                                insert_to_pd(dict)

                            m21 = moneySum(person_zy, "21")
                            m22 = moneySum(person_zy, "22")
                            if A_member['A108'] < 1.5 and m21 + m22 > 15000:
                                dict['code'] = "ChangH={},TaskCode={},A108={},'21'+'22'={}>15000".format(ChangeH, TaskCode,A_member['A108'], m21+m22)
                                dict['提示信息'] = "本季度工作时间段，但按月收入很高"
                                insert_to_pd(dict)
                            m24 = moneySum(person_zy, "2401")
                            a = A_member['A119']
                            if a == 2 and m21 + m22 + m24 > 0:
                                dict['code'] = "ChangH={},TaskCode={},A119={},'21'+'22'+'2401'={}".format(ChangeH,TaskCode,a,m21 + m22 +m24)
                                dict['提示信息'] = "本季度非常住人员有工资性收入、经营性收入和离退休金"
                                insert_to_pd(dict)
                            m = moneySum(person_zy, "2406")
                            if a == 1 and m > 0:
                                dict['code'] = "ChangH={},TaskCode={},A119={},'2406'={}".format(ChangeH,TaskCode,a,m)
                                dict['提示信息'] = "本季度常住人员有寄带回收入"
                                insert_to_pd(dict)

                            if a == 2 and m > 12000:
                                dict['code'] = "ChangH={},TaskCode={},A119={},'2406'={}".format(ChangeH, TaskCode, a, m)
                                dict['提示信息'] = "本季度非常住人员大量寄带回，请核实"
                                insert_to_pd(dict)

            # 收入、成本相关
            if TaskCode == 7 and ChangeH == 0 and Table(one_zhuhu,'M211') == 2:
                money = moneySum(zy_family,"11") + moneySum(zy_family,"12") + moneySum(zy_family,"13") + moneySum(zy_family,"14")
                if money > 500:
                    dict['code'] = "ChangH={},TaskCode={},M211={},'11’~‘14'={}".format(ChangeH, TaskCode, Table(one_zhuhu,'M211'), money)
                    dict['提示信息'] = "不是农业经营户有超过500元的农业生产经营账页数据"
                    insert_to_pd(dict)
            mm = moneySum(zy_family, "22") + moneySum(zy_family, "51")
            if ChangeH == 0 and Table(one_zhuhu,'M212') == 2 and mm > 0:
                dict['code'] = "ChangH={},M212={},'22'+'51'={}".format(ChangeH, Table(one_zhuhu,'M212'), mm)
                dict['提示信息'] = "不是二三产经营户有二三产经营账页数据"
                insert_to_pd(dict)


    end = datetime.datetime.now()
    print("运行时间：",end - begin)
    return zy_addition_result


# 打开csv文件 返回DataFrame对象
def read_csv(path):
    with open(path, 'r') as f:
        df = pd.read_csv(f, header=0, low_memory=False)
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
    # A_path = u"D:/研一/项目/CheckProgram/Auditing/输入文件夹/A卷310151.182.20181015.csv"
    # B_path = u"D:/研一/审核程序/src/输入文件夹/B310151.18.csv"
    # zy_path = u"D:/研一/项目/CheckProgram/Auditing/输入文件夹/03账页310151.1803.csv"

    A_path = r"D:\研一\项目\测试数据\外部程序测试数据\外部程序测试数据\18年A卷数据.csv"
    B_path = r"D:\研一\项目\测试数据\外部程序测试数据\外部程序测试数据\18年B卷数据.csv"
    zy_path = r"D:\研一\项目\测试数据\外部程序测试数据\外部程序测试数据\18年账页数据.csv"
    xiaoqu_path = r"D:\研一\项目\测试数据\外部程序测试数据\外部程序测试数据\S310151.18_1.csv"
    zhuhu_path = u"D:\研一\项目\githubProjects\code\AuditingAPP\输入文件夹\住户样本310151.18.csv"
    zhuzhai_path = u"D:\研一\项目\githubProjects\code\AuditingAPP\输入文件夹\住宅名录310151.18.csv"

    TableA = read_csv(A_path)
    TableB = read_csv(B_path)
    zy = read_csv(zy_path)
    zhuzhai = read_csv(zhuzhai_path)
    zhuhu = read_csv(zhuhu_path)
    xiaoqu = read_csv(xiaoqu_path)

    head = {'year': [], 'task': [], 'month': [], 'scode': [], 'sid': [], 'person': [], 'name': [], 'code': [],
            '核实内容': [], 'townname': [], 'vname': []}
    result = pd.DataFrame(head)
    result = result[['sid', 'year', 'month', 'task', 'scode','person', 'name', 'code', '核实内容', 'townname', 'vname']]
    zy_check_addition(TableA,TableB,zy,zhuzhai,zhuhu,xiaoqu,result)
    zy_addition_result.to_excel('zy_addition_result.xlsx', encoding="utf-8", index=False, sheet_name='Sheet')