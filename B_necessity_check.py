# ********************************
# *   全国住户生活状况调查季报   *
# *    问卷B审核公式（必要性）   *
# *        2015年08月26日        *
# ********************************
import pandas as pd
import datetime
from typing import Any

# Year = datetime.datetime.now().year
# Month = datetime.datetime.now().month
# global i = 2
# 打开csv文件 返回DataFrame对象


def read_csv(path):
    with open(path, 'r') as f:
        file = pd.read_csv(f, header=0)
    return file

# M_path = "D:\研一\审核程序\src\输入文件夹\M310151.摸底.csv"
# fp3 = open(M_path)
# tableM = read_file(fp3)


def Table(table, code):
    t = table[code]
    # print("tabledata:",t)
    if t.empty == False:
        if type(t.values[0]) == type("str"):
            # print("字符串类型：",type(t.values[0]))
            return int(t.values[0])
        return t.values[0]
    else:
        return 0


def B_necessity_check(tableB,zhuhu,zhuzhai):
    B,M = 92,993
    E = 95
    # M_path = "D:\研一\审核程序\src\输入文件夹\M310151.摸底.csv"
    # fp3 = open(M_path)
    # tableM = read_file(fp3)
    #if M_91 == M_94:  #//开户拒访：开户时间和拒访时间相同
    #if tableB['M91'] == tableB['M94']:
     #   nextHousehold

    # B1部分    住房基本情况
    #B1.1    期末现住房基本情况
    #超界错误。b101 - b116重写，0825lw
    #B101 ?: "B101填报错误"
    #if tableB['B101']:
    result = open(r'D:\研一\审核程序\src\审核结果输出\B_necessity_CheckResult.txt', 'w')
    for row in tableB.iterrows():
        single_row = row[1]
        #zhuzhai对应M1，zhuhu对应M2
        one_zhuhu = zhuhu[zhuhu['HHID'] == single_row['SID']]
        one_zhuzhai = zhuzhai[zhuzhai['HID'] == single_row['SID'][:-2]]
        # Year = Table(single_row, "YEAR")
        Year = int(single_row['YEAR'])
        result.write(single_row['SID'])
        # if pd.isnull(single_row['B101']) == False:
        if single_row['B101'] != 0:
            # print('aaaa')
            if single_row['B101'] < 1 or single_row['B101'] > 4:
                result.write('住宅类型越界，请填写（1-4）')
            if single_row['B102'] < 1 or single_row['B102'] > 8:
                result.write('本住宅的建筑样式越界，请填写（1-8）')
            if single_row['B103'] < 1 or single_row['B103'] > 5:
                result.write('主要建筑材料越界，请填写（1-5）')
            if single_row['B104'] < 1 or single_row['B104'] > 11:
                result.write('房屋来源越界，请填写（1-11）')
            if single_row['B105'] <= 0 or single_row['B105'] > 999:
                result.write('建筑面积填报越界，请核实')
            if single_row['B106'] <1 or single_row['B106'] > 3:
                result.write('住宅外道路路面情况越界，请填写（1-3）')
            if single_row['B107'] <1 or single_row['B107'] > 3:
                result.write('住宅内是否有管道自来水越界，请填写（1-3）')
            if single_row['B108'] < 1 or single_row['B108'] > 7:
                result.write('主要饮用水来源越界，请填写（1-7））')
            if single_row['B109'] < 1 or single_row['B109'] > 4:
                result.write('获取饮用水存在的困难越界，请填写（1-4）')
            if single_row['B110'] < 1 or single_row['B110'] > 5:
                result.write('饮用水处理措施越界，请填写（1-5）')
            if single_row['B111'] < 1 or single_row['B111'] > 5:
                result.write('厕所类型越界，请填写（1-5）')
            if single_row['B112'] < 1 or single_row['B112'] > 3:
                result.write('厕所使用情况越界，请填写（1-3）')
            if single_row['B113'] < 1 or single_row['B113'] > 4:
                result.write('洗澡设施越界，请填写（1-4）')
            if single_row['B114'] < 1 or single_row['B114'] > 3:
                result.write('主要取暖设备状况越界，请填写（1-3）')
            if single_row['B115'] < 1 or single_row['B115'] > 11:
                result.write('主要取暖能源状况越界，请填写（1-11）')
            if single_row['B116'] < 1 or single_row['B116'] > 11:
                result.write('主要灶用能源状况越界，请填写（1-11）')

            #逻辑审核
            if single_row['B102'] == 8:
                if single_row['B103'] == 1 or single_row['B103'] == 2:
                    result.write('居住空间样式为其他，建筑材料不应为钢混或砖混结构')
            if single_row['B107'] == 1 and single_row['B109'] == 1:
                result.write('有自来水，单次取水时间却超过半小时？')

            #跳转审核
            if single_row['B104'] >= 3 and single_row['B104'] <= 8:
                if single_row['B117']+single_row['B118']+single_row['B119']+single_row['B120']+single_row['B121']+single_row['B122']+single_row['B123']+single_row['B124']+single_row['B125']+single_row['B126']<= 0:
                    result.write('现住房为自有房者应填写B117-B126')
            # if single_row['B104'] == 1 or single_row['B104'] == 2:
            #     if tableM['M101'] <= 1 and tableM['M205'] == 4:
            #         if single_row['B127'] <= 0:
            #             result.write('现住房为租赁房者应填写B127')
            if single_row['B104'] == 1 or single_row['B104'] == 2:
                if Table(one_zhuzhai, 'M101') <= 1 and Table(one_zhuhu,'M205') == 4:
                    if tableB['B127'] <= 0:
                        result.write('现住房为租赁房者应填写B127')
        # print('ok')
        # df = tableM[single_row['SID'][:-2] == tableM['SID']]
        # for index, Modi in df.iterrows():
            # if single_row['SID'][:-2] == Modi['SID']:
        # if pd.isnull(single_row['B101']) == False:
            # 住家保姆、帮工或者集体居住户审核
        if single_row['B101'] != 0:
            if Table(one_zhuhu,'M205') == 4:
                # value = single_row.apply(lambda : x['B117']+x['B118'], axis=1)
                if single_row['B117'] + single_row['B118'] + single_row['B119'] + single_row['B120'] + single_row['B121'] + single_row['B122'] \
                        + single_row['B123'] + single_row['B124'] + single_row['B125'] + single_row['B126'] + single_row['B127'] + single_row[
                    'B128'] \
                        + single_row['B129'] + single_row['B130'] + single_row['B131'] + single_row['B132'] + single_row['B133'] + single_row[
                    'B134'] \
                        + single_row['B135'] + single_row['B136'] + single_row['B137'] + single_row['B138'] + single_row['B139'] + single_row[
                    'B140'] \
                        + single_row['B141'] + single_row['B142'] + single_row['B143'] + single_row['B144'] + single_row['B145'] + single_row[
                    'B146'] \
                        + single_row['B147'] + single_row['B148'] + single_row['B149'] + single_row['B150'] > 0:
                    result.write('住家保姆、帮工,不用填B117-B150')
                if single_row['B101'] != 4:
                    result.write('住家保姆、帮工,B101要填4')
                if single_row['B104'] != 10:
                    result.write('住家保姆、帮工,B104要填10')
                if single_row['B112'] != 2:
                    result.write('住家保姆、帮工,B112要填2')

        if Table(one_zhuzhai,'M101') == 1 and single_row['B101'] != 0:
            # B1.2 自有现住房情况
            if single_row['B104'] >= 3 and single_row['B104'] <= 8:
                if single_row['B118'] < 0.5 or single_row['B118'] >= 999:
                    result.write('B118自有现住房市场价越界')
                if single_row['B119'] < 100 or single_row['B119'] >= 9999:
                    result.write('B119同类住房的市场价月租金越界')
                if single_row['B120'] < 1949 or single_row['B120'] > Year:
                    result.write('B120现住房购(建)房时间越界')
                if single_row['B121'] < 0.5 or single_row['B121'] >= 999:
                    result.write('B121购(建)房总金额越界')
                if single_row['B122'] != 0:
                    if single_row['B122'] < 0.1 or single_row['B122'] >= 999:
                        result.write('B122购(建)房时借贷总额(不含利息)越界')
                    if single_row['B125'] < 1 or single_row['B125'] > 30:
                        result.write('B125借贷款还款总年限越界')
                if pd.isnull(single_row['B123']) == False:
                    if single_row['B123'] < 1 or single_row['B123'] >= 999:
                        result.write('B123购(建)房按揭贷款越界')
                if pd.isnull(single_row['B124']) == False:
                    if single_row['B124'] < 0.01 or single_row['B124'] >= 99:
                        result.write('B124购(建)房时借贷款总利息越界')
                if pd.isnull(single_row['B125']) == False:
                    if single_row['B126'] != 1 and single_row['B126'] != 2:
                        result.write('B126现在是否还在还款越界')
                if single_row['B121'] < single_row['B122']:
                    result.write('借贷款总额不应大于购(建)房总金额')
                if single_row['B122'] < single_row['B123']:
                    result.write('按揭贷款不应大于借贷款总额')
                if single_row['B122'] == 0:
                    if pd.isnull(single_row['B124']) == False:
                        result.write('没借贷款不应有利息')
                    if pd.isnull(single_row['B125']) == False:
                        result.write('没借贷款不应填还款总年限')
                    if pd.isnull(single_row['B126']) == False:
                        result.write('没借贷款不应填写B126')
            # B1.3 期内拥有其它房屋情况
            if single_row['B128'] > 0:
                if single_row['B128'] < 5 or single_row['B128'] > 999:
                    result.write('B128出租住房建筑面积填报越界')
                if single_row['B129'] < 0.3 or single_row['B129'] >= 999:
                    result.write('B129出租住房市场价越界')
                if single_row['B130'] < 100 or single_row['B130'] >= 9999:
                    result.write('B130出租住房月租金越界')
            if single_row["B131"] > 0:
                if single_row['B131'] < 1 or single_row['B131'] > 999:
                    result.write('B131出租商用建筑物建筑面积填报越界')
                if single_row['B132'] < 0.3 or single_row['B132'] >= 999:
                    result.write('B132出租商用建筑物市场价越界')
            if single_row["B134"] > 0:
                if single_row['B134'] < 1 or single_row['B134'] > 999:
                    result.write('B134偶尔居住房建筑面积填报越界')
                if single_row['B135'] < 0.3 or single_row['B135'] >= 999:
                    result.write('B135偶尔居住房市场价越界')
            if single_row["B136"] > 0:
                if single_row['B136'] < 1 or single_row['B136'] > 999:
                    result.write('B136空宅或其他用途住房建筑面积填报越界')
                if single_row['B137'] < 0.3 or single_row['B137'] >= 999:
                    result.write('B137空宅或其他用途住房市场价越界')

            # B1.4新购住房情况审核
            if single_row['B104'] >= 4 and single_row['B104'] <= 7 and single_row['B120'] == Year:
                if single_row['B138'] < 5 or single_row['B138'] >= 999:
                    result.write('新购住房建筑面积越界')
                if single_row['B140'] != 0:
                    if single_row['B140'] < 1 or single_row['B140'] >= 999:
                        result.write('新购住房借贷款总额(不含利息)越界')
                    if single_row['B141'] < 1 or single_row['B141'] >= 999:
                        result.write('新购住房按界揭贷款越')
                    if single_row['B142'] < 1 or single_row['B142'] >= 99:
                        result.write('新购住房借贷款总利息越界')
                    if single_row['B143'] < 1 or single_row['B143'] > 30:
                        result.write('新购住房借贷款还款总年限越界')
                    if single_row['B142'] / single_row['B140'] < 0 or single_row['B142'] / single_row['B140'] >= 0.15:
                        result.write('贷款利率越界')
                    if single_row['B143'] < 3 or single_row['B143'] > 30:
                        result.write('还款年限越界')
                if single_row['B139'] <= single_row['B140']:
                    result.write('借贷款总额不应大于购(建)房总金额')
                if single_row['B140'] < single_row['B141']:
                    result.write('按揭贷款不应大于借贷款总额')
                if single_row['B140'] == 0:
                    if single_row['B142'] != 0:
                        result.write('没借贷款不应有利息')
                    if single_row['B143'] != 0:
                        result.write('没借贷款不应填还款总年限')

            # B1.5 新住房情况审核
            if single_row['B104'] == 3 and single_row['B120'] == Year:
                if single_row['B144'] != 0:
                    if single_row['B144'] < 5 or single_row['B144'] >= 999:
                        result.write('新住房建筑面积越界')
                if single_row['B145'] != single_row['B146'] + single_row['B147'] + single_row['B148'] + single_row['B149']:
                    result.write('建房资金来源不平')

            # B1.6 期内住房大修或装修费用
            if single_row['B150'] > 99:
                result.write('住房大修或装修费用越界')
            # break
        #B2部分 耐用消费品情况
        # if single_row['B101']:
        if pd.isnull(single_row['B101']) == False:
            # print('ccc')
            if single_row['B201'] < 0 or single_row['B201'] > 3:
                result.write('B201耐用消费品拥有量超界，请核实')
            if single_row['B202'] < 0 or single_row['B202'] > 3:
                result.write('B202耐用消费品拥有量超界，请核实')
            if single_row['B203'] < 0 or single_row['B203'] > 5:
                result.write('B203耐用消费品拥有量超界，请核实')
            if single_row['B204'] < 0 or single_row['B204'] > 5:
                result.write('B204耐用消费品拥有量超界，请核实')
            if single_row['B205'] < 0 or single_row['B205'] > 5:
                result.write('B205耐用消费品拥有量超界，请核实')
            if single_row['B206'] < 0 or single_row['B206'] > 5:
                result.write('B206耐用消费品拥有量超界，请核实')
            if single_row['B207'] < 0 or single_row['B207'] > 5:
                result.write('B207耐用消费品拥有量超界，请核实')
            if single_row['B208'] < 0 or single_row['B208'] > 10:
                result.write('B208耐用消费品拥有量超界，请核实')
            if single_row['B209'] < 0 or single_row['B209'] > 7:
                result.write('B209耐用消费品拥有量超界，请核实')
            if single_row['B210'] < 0 or single_row['B210'] > 3:
                result.write('B210耐用消费品拥有量超界，请核实')
            if single_row['B211'] < 0 or single_row['B211'] > 5:
                result.write('B211耐用消费品拥有量超界，请核实')
            #if single_row['B212'] < 0 or single_row['B212'] > 5:
             #   result.write('B212耐用消费品拥有量超界，请核实')
            if single_row['B213'] < 0 or single_row['B213'] > 2:
                result.write('B213耐用消费品拥有量超界，请核实')
            if single_row['B214'] < 0 or single_row['B214'] > 2:
                result.write('B214耐用消费品拥有量超界，请核实')
            if single_row['B215'] < 0 or single_row['B215'] > 3:
                result.write('B215耐用消费品拥有量超界，请核实')
            if single_row['B216'] < 0 or single_row['B216'] > 10:
                result.write('B216耐用消费品拥有量超界，请核实')
            if single_row['B217'] < 0 or single_row['B217'] > 10:
                result.write('B217耐用消费品拥有量超界，请核实')
            if single_row['B218'] < 0 or single_row['B218'] > 10:
                result.write('B218耐用消费品拥有量超界，请核实')
            if single_row['B219'] < 0 or single_row['B219'] > 10:
                result.write('B219耐用消费品拥有量超界，请核实')
            #if single_row['B220'] < 0 or single_row['B220'] > 5:
             #   result.write('B220耐用消费品拥有量超界，请核实')
            if single_row['B221'] < 0 or single_row['B221'] > 5:
                result.write('B221耐用消费品拥有量超界，请核实')
            if single_row['B222'] < 0 or single_row['B222'] > 5:
                result.write('B222耐用消费品拥有量超界，请核实')
            if single_row['B223'] < 0 or single_row['B223'] > 5:
                result.write('B223耐用消费品拥有量超界，请核实')
            #if single_row['B224'] < 0 or single_row['B224'] > 5:
             #   result.write('B224耐用消费品拥有量超界，请核实')
            if single_row['B225'] < 0 or single_row['B225'] > 5:
                result.write('B225耐用消费品拥有量超界，请核实')
            if single_row['B226'] < 0 or single_row['B226'] > 5:
                result.write('B226耐用消费品拥有量超界，请核实')
            if single_row['B208'] > single_row['B207']:
                result.write('B208拥有量不应超过B207，请核实')
            if single_row['B211'] > single_row['B210']:
                result.write('B211拥有量不应超过B210，请核实')
            if single_row['B217'] > single_row['B216']:
                result.write('B217拥有量不应超过B216，请核实')
            if single_row['B219'] > single_row['B218']:
                result.write('B219拥有量不应超过B218，请核实')

        #B3部分 补充资料2：现住房房屋状况
        # if single_row['B101']:
            if single_row['B238'] < 1 or single_row['B238'] > 4:
                result.write('住户现住房所处场地状况越界，请填写(1-4)')
            if single_row['B239'] < 1 or single_row['B239'] > 4:
                result.write('住户现住房屋安全状况越界，请填写(1-4)')
            if single_row['B240'] != 1 and single_row['B240'] != 2:
                result.write('住宅地面是否经常有泥土、沙土、畜禽粪便等脏东西越界，请填写(1-2)')

        #B3部分 补充资料3：家庭或家庭成员合伙、参股或独立控股公司（企业）的经营情况 *     lw 0905
        # if single_row['B101']:
            if single_row['B241'] != 1 and single_row['B241'] != 2:
                result.write('是否拥有独立控股的公司（企业）越界，请填写(1-2)')
            if single_row['B243'] != 1 and single_row['B243'] != 2:
                result.write('是否拥有合伙或参股的公司（企业）越界，请填写(1-2)')
            if single_row['B242'] > 0:
                if single_row['B241'] != 1:
                    result.write('有公司（企业）税后净利润，应有公司')
            if single_row['B244'] > 0:
                if single_row['B243'] != 1:
                    result.write('有公司（企业）税后净利润，应有公司')
        result.write('\n')
        # print('done!')
    result.close()


if __name__ == "__main__":
    B_path = "D:\研一\审核程序\src\输入文件夹\B310151.18.csv"
    tableB = read_csv(B_path)
    zhuhu_path = u"\研一\审核程序\src\输入文件夹\住户样本310151.18.csv"
    zhuzhai_path = u"\研一\审核程序\src\输入文件夹\住宅名录310151.18.csv"

    zhuzhai = read_csv(zhuzhai_path)
    zhuhu = read_csv(zhuhu_path)
    zhuzhai = read_csv(zhuzhai_path)
    zhuhu = read_csv(zhuhu_path)

    B_necessity_check(tableB,zhuhu,zhuzhai)
    # M_path = "D:\研一\审核程序\src\输入文件夹\M310151.摸底.csv"
    # fp3 = open(M_path)
    # tableM = read_file(fp3)
    #df = read_file("G:/testData/B310151.18.csv")
    #print(df.index)
    # print(tableB.index)
    # i = 2
    # # result.write(df)
    # for row in tableB.iterrows():
    #     result.write('第%d行数据：\n'%i)
    #     i += 1
    #     B_necessity_check(row[1])
    #     # result.write(row[1]["A101"])
    #     result.write('\n')
    # result.close()