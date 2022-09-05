import pyodbc
import time
import datetime
import requests
from urllib.request import Request
import urllib.request, json
import pandas as pd
import csv
import xlwings as xw
from threading import Thread
import warnings
warnings.filterwarnings("ignore")

wb_key = ''
wb_ip_key = ''

oz_key = ''
oz_id = ''

ya_id = ''
ya_pass = ''
ya_oauth_token = ''

query_WbOrders = '''INSERT INTO tblWbOrders (date, lastChangeDate, supplierArticle, techSize, barcode,totalPrice, 
discountPercent, warehouseName, oblast, incomeID, odid, nmId, subject, category, brand, isCancel, cancel_dt, gNumber, 
sticker, srid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
query_WbSells = '''INSERT INTO tblWbSells (lastChangeDate,date,supplierArticle,techSize,barcode,totalPrice,discountPercent,isSupply,
isRealization,promoCodeDiscount,warehouseName,countryName,oblastOkrugName,regionName,incomeID,saleID,odid,spp,forPay,
finishedPrice,priceWithDisc,nmId,subject,category,brand,IsStorno,gNumber,sticker,srid) 
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
query_WbStocks = ''' INSERT INTO tblWbStocks (lastChangeDate,supplierArticle,techSize,barcode,quantity,isSupply,
isRealization,quantityFull,quantityNotInOrders,warehouse,warehouseName,inWayToClient,inWayFromClient,nmId,subject,
category,daysOnSite,brand,SCCode,Price,Discount)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''
query_WbReportDetailByPeriod = ''' INSERT INTO tblWbReportDetailByPeriod (date_from,date_to,realizationreport_id,suppliercontract_code,
rrd_id,gi_id,subject_name,nm_id,brand_name,sa_name,ts_name,barcode,doc_type_name,quantity,retail_price,retail_amount,
sale_percent,commission_percent,office_name,supplier_oper_name,order_dt,sale_dt,rr_dt,shk_id,retail_price_withdisc_rub,
delivery_amount,return_amount,delivery_rub,gi_box_type_name,product_discount_for_report,supplier_promo,rid,ppvz_spp_prc,
ppvz_kvw_prc_base,ppvz_kvw_prc,ppvz_sales_commission,ppvz_for_pay,ppvz_reward,ppvz_vw,ppvz_vw_nds,ppvz_office_id,
ppvz_supplier_id,ppvz_supplier_name,ppvz_inn,declaration_number,sticker_id,site_country,penalty,
additional_payment,srid)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''

query_WbIPOrders = '''INSERT INTO tblWbIPOrders (date, lastChangeDate, supplierArticle, techSize, barcode,totalPrice, 
discountPercent, warehouseName, oblast, incomeID, odid, nmId, subject, category, brand, isCancel, cancel_dt, gNumber, 
sticker, srid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
query_WbIPSells = '''INSERT INTO tblWbIPSells (lastChangeDate,date,supplierArticle,techSize,barcode,totalPrice,discountPercent,isSupply,
isRealization,promoCodeDiscount,warehouseName,countryName,oblastOkrugName,regionName,incomeID,saleID,odid,spp,forPay,
finishedPrice,priceWithDisc,nmId,subject,category,brand,IsStorno,gNumber,sticker,srid) 
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
query_WbIPStocks = ''' INSERT INTO tblWbIPStocks (lastChangeDate,supplierArticle,techSize,barcode,quantity,isSupply,
isRealization,quantityFull,quantityNotInOrders,warehouse,warehouseName,inWayToClient,inWayFromClient,nmId,subject,
category,daysOnSite,brand,SCCode,Price,Discount)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''
query_WbIPReportDetailByPeriod = ''' INSERT INTO tblWbIPReportDetailByPeriod (date_from,date_to,realizationreport_id,suppliercontract_code,
rrd_id,gi_id,subject_name,nm_id,brand_name,sa_name,ts_name,barcode,doc_type_name,quantity,retail_price,retail_amount,
sale_percent,commission_percent,office_name,supplier_oper_name,order_dt,sale_dt,rr_dt,shk_id,retail_price_withdisc_rub,
delivery_amount,return_amount,delivery_rub,gi_box_type_name,product_discount_for_report,supplier_promo,rid,ppvz_spp_prc,
ppvz_kvw_prc_base,ppvz_kvw_prc,ppvz_sales_commission,ppvz_for_pay,ppvz_reward,ppvz_vw,ppvz_vw_nds,ppvz_office_id,
ppvz_supplier_id,ppvz_supplier_name,ppvz_inn,declaration_number,sticker_id,site_country,penalty,
additional_payment,srid)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''

query_OzSellerProducts = '''INSERT INTO tblOzSellerProducts (Артикул ,Ozon_Product_ID ,FBO_OZON_SKU_ID ,FBS_OZON_SKU_ID 
,Barcode ,Наименование_товара ,Контент_рейтинг ,Бренд ,Статус_товара ,Видимость_FBO ,Причины_скрытия_FBO__при_наличии_ 
,Видимость_FBS ,Причины_скрытия_FBS__при_наличии_ ,Дата_создания ,Коммерческая_категория ,Объем_товара__л ,
Объемный_вес__кг ,Доступно_на_складе_Ozon__шт ,Вывезти_и_нанести_КИЗ__кроме_Твери___шт ,Зарезервировано__шт ,
Доступно_на_моих_складах__шт ,Зарезервировано_на_моих_складах__шт ,Текущая_цена_с_учетом_скидки__руб_ ,
Цена_до_скидки__перечеркнутая_цена___руб_ ,Цена_Premium__руб_ ,Рыночная_цена__руб_ ,Актуальная_ссылка_на_рыночную_цену
 ,Размер_НДС___ ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
query_OzSellerTransactions = '''INSERT INTO tblOzSellerTransactions (Дата_начисления ,Тип_операции ,
Номер_отправления_или_идентификатор_услуги ,Склад_отгрузки ,Дата_принятия_заказа_в_обработку_или_совершения_услуги 
,Список_SKU ,Список_товаров_или_название_услуги ,Цена_товаров_в_отправлении ,Комиссия ,Плата_за_доставку 
,Плата_за_доставку_возврата ,Итого )
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''
query_OzSellerProductPrices = '''INSERT INTO tblOzSellerProductPrices (Артикул ,Ozon_SKU_ID ,Название ,Статус 
,Видимость_на_OZON ,Объемный_вес__кг ,Размер_комиссии___ ,Минимальная_сумма_комиссии__руб_ ,
Тариф_за_доставку_со_склада_OZON__руб_ ,Тариф_за_доставку_со_склада_продавца__руб_ ,НДС___ ,Цена_до_скидки__руб_ 
,Текущая_цена__со_скидкой___руб_ ,Скидка___ ,Скидка__руб_ ,Цена_с_учетом_акции__руб_ ,Скидка____1 ,Скидка__руб__1 
,Цена_с_Ozon_Premium__руб_ ,Рыночная_цена__руб_ ,Ценовой_индекс_товара 
,Настройка_автоматического_применения_рыночной_цены ,Минимальное_значение_рыночной_цены__руб_ ,
Ссылка_на_рыночную_цену ,Настройка_предоплаты ,Настройка_автоматического_добавления_в_акции ) 
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
query_OzSellerStock = '''INSERT INTO tblOzSellerStock (Артикул ,Ozon_Product_ID ,OZON_SKU_ID ,Наименование_товара 
,Barcode ,Статус_товара ,Видимость_сайте ,Всего_доступно_на_складах_Ozon__шт ,Всего_зарезервировано__шт )
VALUES (?,?,?,?,?,?,?,?,?)'''
query_OzSellerProductMovement = '''INSERT INTO tblOzSellerProductMovement (Дата ,SKU ,Артикул_продавца ,Название_товара 
,Количество ,Номер_заказа ,Входящий_склад ,Исходящий_склад ,Тип_движения  )
VALUES (?,?,?,?,?,?,?,?,?)'''
query_OzSellerReturns = '''INSERT INTO tblOzSellerReturns (ID_товара_в_возврате ,ID_отправления ,Номер_отправления 
,Статус ,Дата_возврата ,Артикул_товара ,Ozon_ID ,Название_товара ,Количество_возвращаемых_товаров ,Стоимость_товара 
,Причина_возврата ,Стоимость_размещения ,Переход_в_Готов_к_получению ,Последний_день_бесплатного_размещения 
,Дата_возврата_продавцу ,Местоположение ,Отправление_вскрыто ,Процент_комиссии ,Комиссия__руб_ ,Цена_без_комиссии__руб_ 
,Перемещается ,Целевое_место_назначения ,Кол_во_дней_хранения ,Стоимость_доставки )
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
query_OzSellerPostings = '''INSERT INTO tblOzSellerPostings (Номер_заказа ,Номер_отправления ,Дата_и_время_заказа 
,Дата_отгрузки ,Статус ,Стоимость ,Наименование_товара ,Озон_ID ,Артикул ,Цена ,Количество )
VALUES (?,?,?,?,?,?,?,?,?,?,?)'''
query_OzSellerFinance = '''INSERT INTO tblOzSellerFinance (Дата_открытия ,Дата_закрытия ,Баланс_на_начало_периода 
,Баланс_на_конец_периода ,Сумма_заказов ,Сумма_возвратов ,Комиссия ,Сумма_услуг ,Сумма_штрафов )
VALUES (?,?,?,?,?,?,?,?,?)'''

query_YaOrders = '''INSERT INTO tblYaOrders (id,creationDate,status,statusUpdateDate,partnerOrderId,paymentType
,deliveryRegion,items,payments,commissions) VALUES (?,?,?,?,?,?,?,?,?,?)'''
query_YaStocks = ''' INSERT INTO tblYaStocks (shopSku,marketSku,name,price,categoryId,categoryName,
weightDimensions,warehouses,tariffs) VALUES (?,?,?,?,?,?,?,?,?) '''


global prev_week
prev_week = datetime.datetime.today()
global next_week
next_week = prev_week + datetime.timedelta(weeks=1)
global p_w
p_w = prev_week

def get_current_time():
    today = datetime.datetime.today() - datetime.timedelta(minutes=300)
    return today.strftime("%Y-%m-%dT%H:%M:%S")


def wb_json_to_csv(data, filename):
    pokedex = open(filename, 'w')
    csvwriter = csv.writer(pokedex, lineterminator='\n')
    # вынужденная мера, в отчетах встречаются разные длины возвращаемых словарей(49 и 48), решение-удалить параметр
    # из словаря с длиной 49
    for i in data:
        for j in list(i):
            if j == 'ppvz_office_name':
                i.pop('ppvz_office_name')
            elif j == 'bonus_type_name':
                i.pop('bonus_type_name')
    csvwriter.writerow(data[0].keys())
    for row in data:
        csvwriter.writerow(row.values())
    pokedex.close()


def wbupload(filename, query):
    cnxn = pyodbc.connect("Driver={SQL Server};"
                          "Server=mssql.u1740122.plsk.regruhosting.ru;"
                          "Database=;"
                          "UID=;PWD=")
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    data = pd.read_csv(filename, encoding='cp1251')
    df = pd.DataFrame(data)
    df = df.astype(str)
    if filename == 'WbOrders.csv':
        for row in df.itertuples():
            cursor.execute(query, row.date, row.lastChangeDate, row.supplierArticle, row.techSize, row.barcode,
                           row.totalPrice, row.discountPercent, row.warehouseName, row.oblast, row.incomeID, row.odid,
                           row.nmId, row.subject, row.category, row.brand, row.isCancel, row.cancel_dt, row.gNumber,
                           row.sticker, row.srid)
            cursor.commit()
        cursor.execute("""DELETE T FROM
        (
        SELECT *, DupRank = ROW_NUMBER() OVER
        (PARTITION BY gNumber, odid ORDER BY (SELECT NULL))
        FROM tblWbOrders
        )
        AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'WbSells.csv':
        for row in df.itertuples():
            cursor.execute(query, row.lastChangeDate, row.date, row.supplierArticle, row.techSize, row.barcode,
                           row.totalPrice, row.discountPercent, row.isSupply, row.isRealization, row.promoCodeDiscount,
                           row.warehouseName, row.countryName, row.oblastOkrugName, row.regionName, row.incomeID,
                           row.saleID
                           , row.odid, row.spp, row.forPay, row.finishedPrice, row.priceWithDisc, row.nmId, row.subject,
                           row.category, row.brand, row.IsStorno, row.gNumber, row.sticker, row.srid)
            cursor.commit()
        cursor.execute("""DELETE T FROM
        (
        SELECT *, DupRank = ROW_NUMBER() OVER
        (PARTITION BY gNumber, saleID ORDER BY (SELECT NULL))
        FROM tblWbSells
        )
        AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'WbStocks.csv':
        for row in df.itertuples():
            cursor.execute(query, row.lastChangeDate, row.supplierArticle, row.techSize, row.barcode, row.quantity,
                           row.isSupply, row.isRealization, row.quantityFull, row.quantityNotInOrders, row.warehouse,
                           row.warehouseName, row.inWayToClient, row.inWayFromClient, row.nmId, row.subject,
                           row.category,
                           row.daysOnSite, row.brand, row.SCCode, row.Price, row.Discount)
            cursor.commit()
        cursor.execute("""DELETE T FROM
        (
        SELECT *, DupRank = ROW_NUMBER() OVER
        (PARTITION BY lastChangeDate,barcode,warehouse,nmId ORDER BY (SELECT NULL))
        FROM tblWbStocks
        )
        AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'WbReportDetailByPeriod.csv':
        for row in df.itertuples():
            cursor.execute(query, row.date_from, row.date_to, row.realizationreport_id, row.suppliercontract_code,
                           row.rrd_id, row.gi_id,
                           row.subject_name, row.nm_id, row.brand_name, row.sa_name, row.ts_name, row.barcode,
                           row.doc_type_name, row.quantity, row.retail_price, row.retail_amount, row.sale_percent,
                           row.commission_percent, row.office_name, row.supplier_oper_name, row.order_dt, row.sale_dt,
                           row.rr_dt, row.shk_id, row.retail_price_withdisc_rub, row.delivery_amount, row.return_amount,
                           row.delivery_rub, row.gi_box_type_name, row.product_discount_for_report, row.supplier_promo,
                           row.rid, row.ppvz_spp_prc, row.ppvz_kvw_prc_base, row.ppvz_kvw_prc,
                           row.ppvz_sales_commission,
                           row.ppvz_for_pay, row.ppvz_reward, row.ppvz_vw, row.ppvz_vw_nds, row.ppvz_office_id,
                           row.ppvz_supplier_id, row.ppvz_supplier_name, row.ppvz_inn, row.declaration_number,
                           row.sticker_id, row.site_country, row.penalty, row.additional_payment, row.srid)
            cursor.commit()
        cursor.execute("""DELETE T FROM
        (
        SELECT *, DupRank = ROW_NUMBER() OVER
        (PARTITION BY rrd_id,rid,srid,quantity	 ORDER BY (SELECT NULL))
        FROM tblWbReportDetailByPeriod
        )
        AS T WHERE DupRank > 1""")
        cursor.commit()
    cnxn.close()


def WbOrders():
    current_time = get_current_time()
    orders_link = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/orders?dateFrom=' + current_time + '&key=' + wb_key
    time.sleep(4)
    attemts = 0
    order_data = 0
    while True:
        try:
            with urllib.request.urlopen(orders_link) as url:
                order_data = json.loads(url.read().decode())
            if order_data:
                break
            elif attemts > 3:
                break
            time.sleep(30)
            attemts += 1
        except Exception:
            break
    if order_data != 0:
        wb_json_to_csv(order_data, 'WbOrders.csv')
        wbupload('WbOrders.csv', query_WbOrders)
        print('wborders')


def WbSells():
    current_time = get_current_time()
    time.sleep(4)
    sells_link = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/sales?dateFrom=' + current_time + '&key=' + wb_key
    attemts = 0
    sells_data = 0
    while True:
        try:
            with urllib.request.urlopen(sells_link) as url:
                sells_data = json.loads(url.read().decode())
            if sells_data:
                break
            elif attemts > 3:
                break
            time.sleep(30)
            attemts += 1
        except Exception:
            break
    if sells_data != 0:
        wb_json_to_csv(sells_data, 'WbSells.csv')
        wbupload('WbSells.csv', query_WbSells)
        print('wbsells')


def WbStocks():
    current_time = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
    timeTo = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    stocks_link = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks?dateFrom=' + timeTo + '&key=' + wb_key
    attemts = 0
    stocks_data = 0
    while True:
        try:
            with urllib.request.urlopen(stocks_link) as url:
                stocks_data = json.loads(url.read().decode())
            if stocks_data:
                break
            elif attemts > 3:
                break
            time.sleep(30)
            attemts += 1
        except Exception:
            break
    if stocks_data != 0:
        wb_json_to_csv(stocks_data, 'WbStocks.csv')
        wbupload('WbStocks.csv', query_WbStocks)
        print('wbstocks')


# только за месяц можно
def WbReportDetailByPeriod():
    global prev_week
    global next_week
    if prev_week.strftime("%Y-%m-%d") == datetime.datetime.today().strftime("%Y-%m-%d"):
        current_time = datetime.datetime.today().strftime("%Y-%m-%d")
        timeTo = (datetime.datetime.today() - datetime.timedelta(days=31)).strftime("%Y-%m-%d")
        ReportDetailByPeriod_link = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod?dateFrom=' + timeTo + '&dateTo=' + current_time + '&key=' + wb_key
        attemts = 0
        ReportDetailByPeriod_data = 0
        while True:
            try:
                with urllib.request.urlopen(ReportDetailByPeriod_link) as url:
                    ReportDetailByPeriod_data = json.loads(url.read().decode())
                if ReportDetailByPeriod_data:
                    break
                elif attemts > 3:
                    break
                time.sleep(30)
                attemts += 1
            except Exception:
                break
        if ReportDetailByPeriod_data != 0:
            wb_json_to_csv(ReportDetailByPeriod_data, 'WbReportDetailByPeriod.csv')
            wbupload('WbReportDetailByPeriod.csv', query_WbReportDetailByPeriod)
            print('wbreport')
        prev_week = next_week
        next_week = prev_week + datetime.timedelta(weeks=1)


def wbipupload(filename, query):
    cnxn = pyodbc.connect("Driver={SQL Server};"
                          "Server=mssql.u1740122.plsk.regruhosting.ru;"
                          "Database=;"
                          "UID=;PWD=")
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    data = pd.read_csv(filename, encoding='cp1251')
    df = pd.DataFrame(data)
    df = df.astype(str)
    if filename == 'WbIPOrders.csv':
        for row in df.itertuples():
            cursor.execute(query, row.date, row.lastChangeDate, row.supplierArticle, row.techSize, row.barcode,
                           row.totalPrice, row.discountPercent, row.warehouseName, row.oblast, row.incomeID, row.odid,
                           row.nmId, row.subject, row.category, row.brand, row.isCancel, row.cancel_dt, row.gNumber,
                           row.sticker, row.srid)
            cursor.commit()
        cursor.execute("""DELETE T FROM
        (
        SELECT  *, DupRank = ROW_NUMBER() OVER
        (PARTITION BY gNumber,nmId ORDER BY (SELECT NULL))
        FROM tblWbIPOrders
        )
        AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'WbIPSells.csv':
        for row in df.itertuples():
            cursor.execute(query, row.lastChangeDate, row.date, row.supplierArticle, row.techSize, row.barcode,
                           row.totalPrice, row.discountPercent, row.isSupply, row.isRealization, row.promoCodeDiscount,
                           row.warehouseName, row.countryName, row.oblastOkrugName, row.regionName, row.incomeID,
                           row.saleID
                           , row.odid, row.spp, row.forPay, row.finishedPrice, row.priceWithDisc, row.nmId, row.subject,
                           row.category, row.brand, row.IsStorno, row.gNumber, row.sticker, row.srid)
            cursor.commit()
        cursor.execute("""DELETE T FROM
        (
        SELECT *, DupRank = ROW_NUMBER() OVER
        (PARTITION BY gNumber, saleID ORDER BY (SELECT NULL))
        FROM tblWbIPSells
        )
        AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'WbIPStocks.csv':
        for row in df.itertuples():
            cursor.execute(query, row.lastChangeDate, row.supplierArticle, row.techSize, row.barcode, row.quantity,
                           row.isSupply, row.isRealization, row.quantityFull, row.quantityNotInOrders, row.warehouse,
                           row.warehouseName, row.inWayToClient, row.inWayFromClient, row.nmId, row.subject,
                           row.category,
                           row.daysOnSite, row.brand, row.SCCode, row.Price, row.Discount)
            cursor.commit()
        cursor.execute("""DELETE T FROM
        (
        SELECT  *, DupRank = ROW_NUMBER() OVER
        (PARTITION BY lastChangeDate,barcode,warehouse,nmId,quantity,quantityFull,quantityNotInOrders ORDER BY (SELECT NULL))
        FROM tblWbIPStocks
        )
        AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'WbIPReportDetailByPeriod.csv':
        for row in df.itertuples():
            cursor.execute(query, row.date_from, row.date_to, row.realizationreport_id, row.suppliercontract_code,
                           row.rrd_id, row.gi_id,
                           row.subject_name, row.nm_id, row.brand_name, row.sa_name, row.ts_name, row.barcode,
                           row.doc_type_name, row.quantity, row.retail_price, row.retail_amount, row.sale_percent,
                           row.commission_percent, row.office_name, row.supplier_oper_name, row.order_dt, row.sale_dt,
                           row.rr_dt, row.shk_id, row.retail_price_withdisc_rub, row.delivery_amount, row.return_amount,
                           row.delivery_rub, row.gi_box_type_name, row.product_discount_for_report, row.supplier_promo,
                           row.rid, row.ppvz_spp_prc, row.ppvz_kvw_prc_base, row.ppvz_kvw_prc,
                           row.ppvz_sales_commission,
                           row.ppvz_for_pay, row.ppvz_reward, row.ppvz_vw, row.ppvz_vw_nds, row.ppvz_office_id,
                           row.ppvz_supplier_id, row.ppvz_supplier_name, row.ppvz_inn, row.declaration_number,
                           row.sticker_id, row.site_country, row.penalty, row.additional_payment, row.srid)
            cursor.commit()
        cursor.execute("""DELETE T FROM
        (
        SELECT *, DupRank = ROW_NUMBER() OVER
        (PARTITION BY rrd_id,rid,srid,quantity	 ORDER BY (SELECT NULL))
        FROM tblWbIPReportDetailByPeriod
        )
        AS T WHERE DupRank > 1""")
        cursor.commit()
    cnxn.close()


def WbIPOrders():
    current_time = get_current_time()
    orders_link = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/orders?dateFrom=' + current_time + '&key=' + wb_ip_key
    attemts = 0
    while True:
        try:
            with urllib.request.urlopen(orders_link) as url:
                iporder_data = json.loads(url.read().decode())
            if iporder_data:
                break
            elif attemts > 3:
                break
            time.sleep(30)
            attemts += 1
        except Exception:
            break
    if iporder_data != 0:
        wb_json_to_csv(iporder_data, 'WbIPOrders.csv')
        wbipupload('WbIPOrders.csv', query_WbIPOrders)
        print('WbIPOrders')


def WbIPSells():
    current_time = get_current_time()
    ipsells_link = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/sales?dateFrom=' + current_time + '&key=' + wb_ip_key
    attemts = 0
    ipsells_data = 0
    while True:
        try:
            with urllib.request.urlopen(ipsells_link) as url:
                ipsells_data = json.loads(url.read().decode())
            if ipsells_data:
                break
            elif attemts > 3:
                break
            time.sleep(30)
            attemts += 1
        except Exception:
            break
    if ipsells_data != 0:
        wb_json_to_csv(ipsells_data, 'WbIPSells.csv')
        wbipupload('WbIPSells.csv', query_WbIPSells)
        print('WbIPSells')


def WbIPStocks():
    current_time = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
    timeTo = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    ipstocks_link = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks?dateFrom=' + timeTo + '&key=' + wb_ip_key
    attemts = 0
    ipstocks_data = 0
    while True:
        try:
            with urllib.request.urlopen(ipstocks_link) as url:
                ipstocks_data = json.loads(url.read().decode())
            if ipstocks_data:
                break
            elif attemts > 3:
                break
            time.sleep(30)
            attemts += 1
        except Exception:
            break
    if ipstocks_data != 0:
        wb_json_to_csv(ipstocks_data, 'WbIPStocks.csv')
        wbipupload('WbIPStocks.csv', query_WbIPStocks)
        print('WbIPStocks')


# только за месяц можно
def WbIPReportDetailByPeriod():
    global prev_week
    global next_week
    global p_w
    if p_w.strftime("%Y-%m-%d") == datetime.datetime.today().strftime("%Y-%m-%d"):
        current_time = datetime.datetime.today().strftime("%Y-%m-%d")
        timeTo = (datetime.datetime.today() - datetime.timedelta(days=31)).strftime("%Y-%m-%d")
        ipReportDetailByPeriod_link = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod?dateFrom=' + timeTo + '&dateTo=' + current_time + '&key=' + wb_ip_key
        attemts = 0
        ipReportDetailByPeriod_data = 0
        while True:
            try:
                with urllib.request.urlopen(ipReportDetailByPeriod_link) as url:
                    ipReportDetailByPeriod_data = json.loads(url.read().decode())
                if ipReportDetailByPeriod_data:
                    break
                elif attemts > 3:
                    break
                time.sleep(30)
                attemts += 1
            except Exception:
                break
        if ipReportDetailByPeriod_data != 0:
            wb_json_to_csv(ipReportDetailByPeriod_data, 'WbIPReportDetailByPeriod.csv')
            wbipupload('WbIPReportDetailByPeriod.csv', query_WbIPReportDetailByPeriod)
            print('WbIPReportDetailByPeriod')
        p_w = prev_week



def OzGetReport(url, code, head):
    method = '/v1/report/info'
    body = {
        "code": code
    }
    body = json.dumps(body)
    while True:
        try:
            response_get = requests.post(url + method, headers=head, data=body)
            file_url = json.loads(response_get.text)
            if file_url['result']['status'] == 'success':
                break
            time.sleep(15)
        except ValueError:
            print("Status is not success")
    file_url = file_url['result']['file']
    return file_url


def OzCreateReport(method, body=None):
    url = "https://api-seller.ozon.ru"
    head = {
        "Client-Id": oz_id,  # сюда клиент id
        "Api-Key": oz_key  # Сюда Api-Key
    }
    body = json.dumps(body)
    if body != 'null':
        response_create = requests.post(url + method, headers=head, data=body)
    else:
        response_create = requests.post(url + method, headers=head)
    time.sleep(5)
    data = json.loads(response_create.text)
    code = data['result']['code']
    return url, code, head


def ozupload(filename, query):
    cnxn = pyodbc.connect("Driver={SQL Server};"
                          "Server=mssql.u1740122.plsk.regruhosting.ru;"
                          "Database=;"
                          "UID=;PWD=")
    cursor = cnxn.cursor()
    if filename == 'OzSellerStock.csv':
        data = pd.read_csv(filename, encoding='utf-8', delimiter=';')
        df1 = pd.DataFrame(data)
        df = df1.iloc[:, 0:9]
    elif filename == 'OzSellerReturns.xlsx':
        path = filename
        path2 = "renamed_" + path

        while True:
            try:
                df = pd.read_excel(path, sheet_name='Возвраты', skiprows=range(1, 5), usecols="A:Y", header=1,
                                   engine='openpyxl')
                df = pd.DataFrame(df)
            except Exception as e:
                print("Failed to open workbook; error: ")
                print(e)
                wingsbook = xw.Book(path)
                wingsapp = xw.apps.active
                wingsbook.save(path2)
                wingsapp.quit()
                path = path2
            else:
                break
        # , sheet_name = 'Возвраты', skiprows = range(1, 5), usecols = "A:X", header = 1

    elif '.csv' in filename:
        data = pd.read_csv(filename, encoding='utf-8', delimiter=';')
        df = pd.DataFrame(data)
    elif '.xlsx' in filename:
        path = filename
        path2 = "renamed_" + path

        while True:
            try:
                df = pd.read_excel(path, sheet_name='Товары и цены', engine='openpyxl', skiprows=range(1, 2),
                                   usecols="A:Z", header=1)
                df = pd.DataFrame(df)
            except Exception as e:
                print("Failed to open workbook; error: ")
                print(e)
                wingsbook = xw.Book(path)
                wingsapp = xw.apps.active
                wingsbook.save(path2)
                wingsapp.quit()
                path = path2
            else:
                break
        df.drop(labels=[0], axis=0, inplace=True)
    # df = df.astype(str)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('-', '_')
    df.columns = df.columns.str.replace(',', '_')
    df.columns = df.columns.str.replace('(', '_')
    df.columns = df.columns.str.replace(')', '_')
    df.columns = df.columns.str.replace('.', '_')
    df.columns = df.columns.str.replace('%', '_')
    df.columns = df.columns.str.replace('[', '_')
    df.columns = df.columns.str.replace(']', '_')
    df.columns = df.columns.str.replace('"', '')
    df.fillna('', inplace=True)

    if filename == 'OzSellerProducts.csv':
        for row in df.itertuples():
            cursor.execute(query, row.Артикул, row.Ozon_Product_ID, row.FBO_OZON_SKU_ID, row.FBS_OZON_SKU_ID,
                           row.Barcode, row.Наименование_товара, row.Контент_рейтинг, row.Бренд, row.Статус_товара
                           , row.Видимость_FBO, row.Причины_скрытия_FBO__при_наличии_, row.Видимость_FBS,
                           row.Причины_скрытия_FBS__при_наличии_, row.Дата_создания, row.Коммерческая_категория,
                           row.Объем_товара__л, row.Объемный_вес__кг, row.Доступно_на_складе_Ozon__шт,
                           row.Вывезти_и_нанести_КИЗ__кроме_Твери___шт, row.Зарезервировано__шт,
                           row.Доступно_на_моих_складах__шт, row.Зарезервировано_на_моих_складах__шт,
                           row.Текущая_цена_с_учетом_скидки__руб_, row.Цена_до_скидки__перечеркнутая_цена___руб_,
                           row.Цена_Premium__руб_, row.Рыночная_цена__руб_,
                           row.Актуальная_ссылка_на_рыночную_цену, row.Размер_НДС___)
            cursor.commit()
        cursor.execute("""DELETE T FROM
            (
            SELECT TOP (600) *, DupRank = ROW_NUMBER() OVER
            (PARTITION BY Barcode ORDER BY (SELECT NULL))
            FROM tblOzSellerProducts
            )
            AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'OzSellerTransactions.csv':
        for row in df.itertuples():
            cursor.execute(query, row.Дата_начисления, row.Тип_операции,
                           row.Номер_отправления_или_идентификатор_услуги,
                           row.Склад_отгрузки,
                           row.Дата_принятия_заказа_в_обработку_или_совершения_услуги,
                           row.Список_SKU, row.Список_товаров_или_название_услуги,
                           row.Цена_товаров_в_отправлении, row.Комиссия,
                           row.Плата_за_доставку, row.Плата_за_доставку_возврата, row.Итого)
            cursor.commit()
        cursor.execute("""DELETE T FROM
                   (
                   SELECT TOP (600) *, DupRank = ROW_NUMBER() OVER
                   (PARTITION BY Номер_отправления_или_идентификатор_услуги ORDER BY (SELECT NULL))
                   FROM tblOzSellerTransactions
                   )
                   AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'OzSellerProductPrices.xlsx':
        for row in df.itertuples():
            cursor.execute(query, row.Артикул, row.Ozon_SKU_ID, row.Название, row.Статус, row.Видимость_на_OZON
                           , row.Объемный_вес__кг, row.Размер_комиссии___, row.Минимальная_сумма_комиссии__руб_
                           , row.Тариф_за_доставку_со_склада_OZON__руб_, row.Тариф_за_доставку_со_склада_продавца__руб_
                           , row.НДС___, row.Цена_до_скидки__руб_, row.Текущая_цена__со_скидкой___руб_, row.Скидка___
                           , row.Скидка__руб_, row.Цена_с_учетом_акции__руб_, row.Скидка____1, row.Скидка__руб__1
                           , row.Цена_с_Ozon_Premium__руб_, row.Рыночная_цена__руб_, row.Ценовой_индекс_товара
                           , row.Настройка_автоматического_применения_рыночной_цены
                           , row.Минимальное_значение_рыночной_цены__руб_, row.Ссылка_на_рыночную_цену
                           , row.Настройка_предоплаты, row.Настройка_автоматического_добавления_в_акции)
            cursor.commit()
        cursor.execute("""DELETE T FROM
                   (
                   SELECT TOP (600) *, DupRank = ROW_NUMBER() OVER
                   (PARTITION BY Артикул ORDER BY (SELECT NULL))
                   FROM tblOzSellerProductPrices
                   )
                   AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'OzSellerStock.csv':
        for row in df.itertuples():
            cursor.execute(query, row.Артикул, row.Ozon_Product_ID, row.OZON_SKU_ID, row.Наименование_товара,
                           row.Barcode, row.Статус_товара, row.Видимость_сайте,
                           row.Всего_доступно_на_складах_Ozon__шт, row.Всего_зарезервировано__шт)
            cursor.commit()
        cursor.execute("""DELETE T FROM
                          (
                          SELECT TOP (600) *, DupRank = ROW_NUMBER() OVER
                          (PARTITION BY Артикул ORDER BY (SELECT NULL))
                          FROM tblOzSellerStock
                          )
                          AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'OzSellerProductMovement.csv':
        for row in df.itertuples():
            cursor.execute(query, row.Дата, row.SKU, row.Артикул_продавца, row.Название_товара,
                           row.Количество, row.Номер_заказа, row.Входящий_склад, row.Исходящий_склад,
                           row.Тип_движения)
            cursor.commit()
        cursor.execute("""DELETE T FROM
                          (
                          SELECT TOP (600) *, DupRank = ROW_NUMBER() OVER
                          (PARTITION BY SKU ORDER BY (SELECT NULL))
                          FROM tblOzSellerProductMovement
                          )
                          AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'OzSellerReturns.csv':
        for row in df.itertuples():
            cursor.execute(query, row.ID_товара_в_возврате, row.ID_отправления, row.Номер_отправления, row.Статус,
                           row.Дата_возврата, row.Артикул_товара, row.Ozon_ID, row.Название_товара,
                           row.Количество_возвращаемых_товаров, row.Стоимость_товара, row.Причина_возврата,
                           row.Стоимость_размещения, row.Переход_в_Готов_к_получению,
                           row.Последний_день_бесплатного_размещения, row.Дата_возврата_продавцу, row.Местоположение,
                           row.Отправление_вскрыто, row.Процент_комиссии, row.Комиссия__руб_,
                           row.Цена_без_комиссии__руб_, row.Перемещается, row.Целевое_место_назначения,
                           row.Кол_во_дней_хранения, row.Стоимость_доставки)
            cursor.commit()
        cursor.execute("""DELETE T FROM
                                 (
                                 SELECT TOP (600) *, DupRank = ROW_NUMBER() OVER
                                 (PARTITION BY ID_товара_в_возврате ORDER BY (SELECT NULL))
                                 FROM tblOzSellerReturns
                                 )
                                 AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'OzSellerPostings.csv':
        for row in df.itertuples():
            cursor.execute(query, row.Номер_заказа, row.Номер_отправления, row.Дата_и_время_заказа, row.Дата_отгрузки
                           , row.Статус, row.Стоимость, row.Наименование_товара, row.Озон_ID, row.Артикул,
                           row.Цена, row.Количество)
            cursor.commit()
        cursor.execute("""DELETE T FROM
                                 (
                                 SELECT  *, DupRank = ROW_NUMBER() OVER
                                 (PARTITION BY Номер_отправления, Номер_заказа, Статус ORDER BY (SELECT NULL))
                                 FROM tblOzSellerPostings
                                 )
                                 AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'OzSellerFinance.csv':
        for row in df.itertuples():
            cursor.execute(query, row.Дата_открытия, row.Дата_закрытия, row.Баланс_на_начало_периода,
                           row.Баланс_на_конец_периода, row.Сумма_заказов, row.Сумма_возвратов
                           , row.Комиссия, row.Сумма_услуг, row.Сумма_штрафов)
            cursor.commit()
        cursor.execute("""DELETE T FROM
                                  (
                                  SELECT TOP (600) *, DupRank = ROW_NUMBER() OVER
                                  (PARTITION BY Сумма_заказов ORDER BY (SELECT NULL))
                                  FROM tblOzSellerFinance
                                  )
                                  AS T WHERE DupRank > 1""")
        cursor.commit()
    cnxn.close()


def OzSellerProducts():
    method = "/v1/report/products/create"
    url, code, head = OzCreateReport(method)
    file = OzGetReport(url, code, head)
    opener = urllib.request.build_opener()
    opener.addheaders = [('Client-Id', oz_id), ("Api-Key", oz_key)]
    urllib.request.install_opener(opener)
    urllib.request.urlretrieve(file, "OzSellerProducts.csv")
    ozupload("OzSellerProducts.csv", query_OzSellerProducts)
    print('OzSellerProducts')


def OzSellerTransactions():
    method = "/v1/report/transactions/create"
    current_time = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
    timeTo = (datetime.datetime.today() - datetime.timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    body = {
        "date_from": current_time,
        "date_to": timeTo,
    }
    url, code, head = OzCreateReport(method, body)
    file = OzGetReport(url, code, head)
    opener = urllib.request.build_opener()
    opener.addheaders = [('Client-Id', oz_id), ("Api-Key", oz_key)]
    urllib.request.install_opener(opener)
    urllib.request.urlretrieve(file, "OzSellerTransactions.csv")
    ozupload("OzSellerTransactions.csv", query_OzSellerTransactions)


def OzSellerProductPrices():
    method = "/v1/report/products/prices/create"
    url, code, head = OzCreateReport(method)
    file = OzGetReport(url, code, head)
    opener = urllib.request.build_opener()
    opener.addheaders = [('Client-Id', oz_id), ("Api-Key", oz_key)]
    urllib.request.install_opener(opener)
    urllib.request.urlretrieve(file, "OzSellerProductPrices.xlsx")
    ozupload("OzSellerProductPrices.xlsx", query_OzSellerProductPrices)
    print('OzSellerProductPrices')


def OzSellerStock():
    method = "/v1/report/stock/create"
    url, code, head = OzCreateReport(method)
    file = OzGetReport(url, code, head)
    opener = urllib.request.build_opener()
    opener.addheaders = [('Client-Id', oz_id), ("Api-Key", oz_key)]
    urllib.request.install_opener(opener)
    urllib.request.urlretrieve(file, "OzSellerStock.csv")
    ozupload("OzSellerStock.csv", query_OzSellerStock)
    print('OzSellerStock')


def OzSellerProductMovement():
    method = "/v1/report/products/movement/create"
    current_time = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
    timeTo = (datetime.datetime.today() - datetime.timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    body = {
        "date_from": current_time,
        "date_to": timeTo,
    }
    url, code, head = OzCreateReport(method, body)
    file = OzGetReport(url, code, head)
    opener = urllib.request.build_opener()
    opener.addheaders = [('Client-Id', oz_id), ("Api-Key", oz_key)]
    urllib.request.install_opener(opener)
    urllib.request.urlretrieve(file, "OzSellerProductMovement.csv")
    ozupload("OzSellerProductMovement.csv", query_OzSellerProductMovement)
    print('OzSellerProductMovement')


def OzSellerReturns():
    method = "/v1/report/returns/create"
    url, code, head = OzCreateReport(method)
    file = OzGetReport(url, code, head)
    opener = urllib.request.build_opener()
    opener.addheaders = [('Client-Id', oz_id), ("Api-Key", oz_key)]
    urllib.request.install_opener(opener)
    urllib.request.urlretrieve(file, "OzSellerReturns.xlsx")
    ozupload("OzSellerReturns.xlsx", query_OzSellerReturns)
    print('OzSellerReturns')


def OzSellerPostings():
    method = "/v1/report/postings/create"
    current_time = (datetime.datetime.today() - datetime.timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    timeTo = (datetime.datetime.today() - datetime.timedelta(days=11)).strftime("%Y-%m-%dT%H:%M:%SZ")
    body = {
        "filter": {
            "processed_at_from": timeTo,
            "processed_at_to": current_time,
            "delivery_schema": [
                "fbs",
                "fbo",
                "crossborder"
            ],
            "sku": [],
            "cancel_reason_id": [],
            "offer_id": "",
            "status_alias": [],
            "statuses": [],
            "title": ""
        },
        "language": "DEFAULT"
    }
    url, code, head = OzCreateReport(method, body)
    file = OzGetReport(url, code, head)
    opener = urllib.request.build_opener()
    opener.addheaders = [('Client-Id', oz_id), ("Api-Key", oz_key)]
    urllib.request.install_opener(opener)
    urllib.request.urlretrieve(file, "OzSellerPostings.csv")
    ozupload("OzSellerPostings.csv", query_OzSellerPostings)
    print('OzSellerPostings')

def OzSellerFinance():
    method = "/v1/report/finance/create"
    current_time = (datetime.datetime.today() - datetime.timedelta(minutes=10)).strftime("%Y-%m-%d")
    timeTo = (datetime.datetime.today() - datetime.timedelta(days=15)).strftime("%Y-%m-%d")
    body = {
        "date_from": timeTo,
        "date_to": current_time,
    }
    url, code, head = OzCreateReport(method, body)
    file = OzGetReport(url, code, head)
    opener = urllib.request.build_opener()
    opener.addheaders = [('Client-Id', oz_id), ("Api-Key", oz_key)]
    urllib.request.install_opener(opener)
    urllib.request.urlretrieve(file, "OzSellerFinance.csv")
    ozupload("OzSellerFinance.csv", query_OzSellerFinance)
    print('OzSellerFinance')


def ya_json_to_csv(data, filename):
    pokedex = open(filename, 'w')
    csvwriter = csv.writer(pokedex, lineterminator='\n')
    if filename == 'YaStocks.csv':
        for i in data['result']['shopSkus']:
            for j in list(i):
                if j == 'hidings':
                    i.pop('hidings')
            if 'warehouses' not in list(i):
                i['warehouses'] = '0'
        csvwriter.writerow(("shopSku", "marketSku", "name", "price", "categoryId", "categoryName", "weightDimensions",
                            "warehouses", "tariffs"))
        for row in data['result']['shopSkus']:
            csvwriter.writerow((row["shopSku"], row["marketSku"], row["name"], row["price"], row["categoryId"],
                                row["categoryName"], row["weightDimensions"], row["warehouses"], row["tariffs"]))
        pokedex.close()
    else:
        csvwriter.writerow(data['result']['orders'][0].keys())
        for row in data['result']['orders']:
            csvwriter.writerow(row.values())
        pokedex.close()


def yaupload(filename, query):
    cnxn = pyodbc.connect("Driver={SQL Server};"
                          "Server=mssql.u1740122.plsk.regruhosting.ru;"
                          "Database=;"
                          "UID=;PWD=")
    cursor = cnxn.cursor()
    data = pd.read_csv(filename, encoding='cp1251')
    df = pd.DataFrame(data)
    df = df.astype(str)
    if filename == 'YaOrders.csv':
        for row in df.itertuples():
            cursor.execute(query, row.id, row.creationDate, row.status, row.statusUpdateDate,
                           row.partnerOrderId, row.paymentType, row.deliveryRegion, row.items,
                           row.payments, row.commissions)
            cursor.commit()
        cursor.execute("""DELETE T FROM
        (
        SELECT *, DupRank = ROW_NUMBER() OVER
        (PARTITION BY id,status ORDER BY (SELECT NULL))
        FROM tblYaOrders
        )
        AS T WHERE DupRank > 1""")
        cursor.commit()
    elif filename == 'YaStocks.csv':
        for row in df.itertuples():
            cursor.execute(query, row.shopSku, row.marketSku, row.name, row.price, row.categoryId, row.categoryName,
                           row.weightDimensions, row.warehouses, row.tariffs)
            cursor.commit()

        cursor.execute("""DELETE T FROM
        (
        SELECT  *, DupRank = ROW_NUMBER() OVER
        (PARTITION BY shopSku ORDER BY (SELECT NULL))
        FROM tblYaStocks
        )
        AS T WHERE DupRank > 1""")
        cursor.commit()
    cnxn.close()


def YaOrders():
    CampaignURL = 'https://api.partner.market.yandex.ru/v2/campaigns/22593589/stats/orders.json?limit=200'
    headers = {'Authorization': 'OAuth oauth_token=' + ya_oauth_token + ', oauth_client_id=' + ya_id}
    while True:
        try:
            result = requests.post(CampaignURL, headers=headers)
            order_data = json.loads(result.text)
            if order_data['status'] == 'OK':
                break
            time.sleep(15)
        except ValueError:
            print("Status is not success")
    if order_data:
        ya_json_to_csv(order_data, 'YaOrders.csv')
        yaupload('YaOrders.csv', query_YaOrders)
        print('YaOrders')


def get_skusShop():
    skus = 'https://api.partner.market.yandex.ru/v2/campaigns/22593589/offer-mapping-entries.json?limit=200'
    headers = {'Authorization': 'OAuth oauth_token=' + ya_oauth_token + ', oauth_client_id=' + ya_id}
    while True:
        try:
            result = requests.get(skus, headers=headers)
            skus_data = json.loads(result.text)
            if skus_data['status'] == 'OK':
                break
            time.sleep(15)
        except ValueError:
            print("Status is not success")
    skus_list = []
    for i in skus_data['result']['offerMappingEntries']:
        skus_list.append(i['offer']['shopSku'])
    return skus_list


def YaStocks():
    skus_list = get_skusShop()
    CampaignURL = 'https://api.partner.market.yandex.ru/v2/campaigns/22593589/stats/skus.json'
    headers = {'Authorization': 'OAuth oauth_token=' + ya_oauth_token + ', oauth_client_id=' + ya_id}
    body = {
        "shopSkus":
            skus_list
    }

    while True:
        try:
            jsonBody = json.dumps(body, ensure_ascii=False).encode('utf8')
            result = requests.post(CampaignURL, jsonBody, headers=headers)
            stocks_data = json.loads(result.text)
            if stocks_data['status'] == 'OK':
                break
            time.sleep(15)
        except ValueError:
            print("Status is not success")

    if stocks_data:
        ya_json_to_csv(stocks_data, 'YaStocks.csv')
        yaupload('YaStocks.csv', query_YaStocks)
        print('YaStocks')




def WbStart():
    funcs = WbOrders, WbSells, WbStocks, WbReportDetailByPeriod, WbIPOrders, WbIPSells, \
            WbIPStocks, WbIPReportDetailByPeriod
    for f in funcs:
        try:
            f()
        except Exception as e:
            print(e)
            pass

def OzStart():
    funcs = OzSellerProducts, OzSellerTransactions, OzSellerProductPrices, OzSellerStock, OzSellerProductMovement, \
            OzSellerReturns, OzSellerPostings, OzSellerFinance
    for f in funcs:
        try:
            f()
        except Exception as e:
            print(e)
            pass


def YaStart():
    funcs = YaOrders, YaStocks
    for f in funcs:
        try:
            f()
        except Exception as e:
            print(e)
            pass


def func():
    WbStart()
    OzStart()
    YaStart()


if __name__ == '__main__':
    while True:
        print(datetime.datetime.today())
        try:
            func()
            time.sleep(1800)
        except Exception as e:
            print(e)
            time.sleep(180)
        except IndexError as i:
            print(i)
            pass

