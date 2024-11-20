import json
import requests
import time
from bs4 import BeautifulSoup
import mysql.connector
from datetime import datetime
from pytz import timezone
import base
from inflection import underscore,camelize


LIST_URL = "https://www.zjzrzyjy.com/trade/view/landbidding/querylandBidding?currentPage=%d&pageSize=%d&sortWay=desc&sortField=ZYKSSJ"
# "https://www.zjzrzyjy.com/trade/view/landbidding/querylandBidding?currentPage=%d&pageSize=%d&sortWay=desc&sortField=ZYKSSJ"
RESOURCE_URL = "https://www.zjzrzyjy.com/trade/view/landbidding/queryResourceDetail?resourceId=%s"
SLEEP_SECONDS = 1

def httpGet(url):
    err = None
    for tryTimes in range(3):
        try:
            res = requests.get(url)
            return res
        except requests.exceptions.ConnectionError as e:
           print('连接错误，准备重试...')
           err = e 
           time.sleep(SLEEP_SECONDS)
    raise err

def getLandList(pageNumber):
    url = LIST_URL % (pageNumber, 50)
    print(url)
    response = httpGet(url)
    text = response.text
    resObj = json.loads(text)
    return resObj['data']['records']

def ifExistLand(conn, resourceId):
    cursor = conn.cursor()
    sql = "select * from zjgtjy3 where resource_id = '%s'" % resourceId
    cursor.execute(sql)
    exist = False
    if cursor.fetchall():
        exist = True
    cursor.close()
    return exist

def deleteByResourceId(conn, resourceId):
    sql = "delete from zjgtjy3 where resource_id = '%s'" % resourceId
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()

def refreshLandInfo(conn):
    print('刷新存量地块数据')
    sql = "select resource_id, resource_stage from zjgtjy3 z where resource_stage in ('GGQ','GPQ', 'PMGGQ', 'PMXWQ', 'PMJJQ','XCYH')"
    cursor = conn.cursor()
    cursor.execute(sql)
    for row in cursor.fetchall():
        # print(row)
        resourceId = row[0]
        resourceStage = row[1]
        landData = queryResourceDetail(resourceId)
        if landData is None:
            continue
        if(landData['resourceStage'] != resourceStage):
            print(resourceId, ' status updated')
            landInfo = readResourceInfo(landData)
            deleteByResourceId(conn,resourceId)
            insertZjgtjy(conn, landInfo)
        time.sleep(SLEEP_SECONDS)

def queryResourceDetail(resourceId):
    url = RESOURCE_URL % (resourceId)
    print(url)
    response = httpGet(url)
    resObj = json.loads(response.text)
    return resObj.get('data',None)

def getFromDist(data, key):
    if key in data:
        if data[key] == None:
            return ''
        else:
            return data[key]
    else:
        return ''
    
def parseRange(data, key):
    obj = json.loads(data)
    x = ''
    s = ''
    if(key + '_X' in obj):
        x = obj[key + '_X']
    if(key + '_S' in obj):
        s = obj[key + '_S']
    return (x,s)

def parseLandUseDetail(data):
    txtDetail = ''
    details = json.loads(data)
    for detail in details:
        txtDetail = txtDetail + detail['type_BAK']
        txtDetail = txtDetail + ','
        txtDetail = txtDetail + detail['unitofarea'] + '平方米'
        txtDetail = txtDetail + ','
        txtDetail = txtDetail + detail['year'] + '年'
        txtDetail = txtDetail + ';'
    return txtDetail

def readResourceInfo(data):
    land = {}
    land['resourceId'] = getFromDist(data, 'resourceId')
    land['resourceNumber'] = getFromDist(data, 'resourceNumber')
    land['announcementPubTime'] = getFromDist(data, 'announcementPubTime')
    land['hangOutEndTime'] = getFromDist(data, 'hangOutEndTime')
    land['resourceLocation'] = getFromDist(data, 'resourceLocation')
    land['planPurposeSecondType'] = getFromDist(data, 'planPurposeSecondType')
    land['administrativeRegioncode'] = getFromDist(data, 'administrativeRegioncode')
    land['subRegion'] = getFromDist(data, 'subRegion')
    land['assignmentPeriod'] = getFromDist(data, 'assignmentPeriod')
    land['assignmentArea'] = getFromDist(data, 'assignmentArea')
    land['assignmentAreaAre'] = getFromDist(data, 'assignmentAreaAre')
    land['startPrice'] = getFromDist(data, 'startPrice')
    land['margin'] = getFromDist(data, 'margin')
    land['addPriceRange'] = getFromDist(data, 'addPriceRange')
    land['resourceStage'] = getFromDist(data, 'resourceStage')
    land['theUnit'] = getFromDist(data, 'theUnit')
    land['endTime'] = getFromDist(data, 'endTime')
    land['dealPrice'] = getFromDist(data, 'dealPrice')
    plotRatio = parseRange(data['plotRatio'], 'RJL')
    land['plotRatioX'] = plotRatio[0]
    land['plotRatioS'] = plotRatio[1]
    land['resourceName'] = getFromDist(data, 'resourceName')
    land['landUseDetail'] = parseLandUseDetail(getFromDist(data, 'landUseDetail'))

    return land

def insertZjgtjy(conn, landInfo):
    columns = ['resource_id','resource_number','announcement_pub_time','hang_out_end_time',
               'resource_location','plan_purpose_second_type','administrative_regioncode',
               'sub_region','assignment_period','assignment_area','assignment_area_are',
               'start_price','margin','resource_stage','the_unit','end_time','deal_price',
               'plot_ratio_x','plot_ratio_s','resource_name','land_use_detail']
    sql = "insert into zjgtjy3 ("
    for i in range(len(columns)):
        sql = sql + columns[i]
        if i != len(columns) - 1:
            sql = sql + ','
    sql = sql + ") values ("
    for i in range(len(columns)):
        value = landInfo[camelize(columns[i], uppercase_first_letter=False)]
        if not isinstance(value, str):
            value = str(value)
        if value == '':
            sql = sql + " null "
        else:
            sql = sql + "'" + value + "'"
        if i != len(columns) - 1:
            sql = sql + ','
    sql = sql + ")"
    # print(sql)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()

def generateFinalData(conn):
    sql = "drop table zjgtjy3_dist"
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    sql = r"""
    create table zjgtjy3_dist
    select
zt.ztmc as '状态',
case when z.sub_region in ('330203',
'330205',
'330206',
'330211',
'330212','330283', '330213','330201') then '1' else '0' END as '市六区',
administrative_regioncode as '行政区划',
resource_id,
resource_number as '资源编号',
resource_name as '资源名称',
resource_location as '位置',
plan_purpose_second_type as '用途',
land_use_detail as '用途明细',
announcement_pub_time as '公告发布时间',
end_time as '成交时间',
assignment_area as '用地面积',
plot_ratio_s as '容积率',
assignment_area * plot_ratio_s as '建筑面积',
margin as '保证金',
start_price as '起拍价',
round(start_price /(assignment_area*plot_ratio_s) * 10000,2) as '起始单价',
deal_price as '成交价',
round(deal_price/(assignment_area*plot_ratio_s)*10000,2) as '成交单价',
the_unit as '竞得单位',
concat(round((deal_price-start_price)/start_price * 100,1),'%') as '溢价率'
from zjgtjy3 z
left join district d on z.sub_region = d.code 
left join zjzt zt on z.resource_stage = zt.zyjd"""
    cursor.execute(sql)
    conn.commit()
    cursor.close()

def run(config):
    conn = base.getDbConnection(config)
    refreshLandInfo(conn)
    for i in range(int(config['start_page']), int(config['end_page']) + 1):
        print('查询第' + str(i) + '页')
        landList = getLandList(i)
        if len(landList) == 0:
            break
        for land in landList:
            if not ifExistLand(conn, land['resourceId']):
                print('发现新土地信息：', land["resourceId"], land['resourceNumber'])
                landData = queryResourceDetail(land['resourceId'])
                if landData is None:
                    continue
                landInfo = readResourceInfo(landData)
                print(landInfo)
                insertZjgtjy(conn, landInfo)
                time.sleep(SLEEP_SECONDS)
    generateFinalData(conn)
    conn.close()

if __name__ == '__main__':
    config = base.loadConfig()
    run(config)
