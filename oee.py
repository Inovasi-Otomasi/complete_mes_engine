import time
import schedule
from db_func import *
import json

def millis():
    return int(time.time() * 1000)

# def check_requirement(sku_code,delta_item_counter=1):
#     sku=db_fetchone('select * from sku_list where sku_code="%s"'%sku_code)
#     req=json.loads(sku['material'])
#     for inv in req:
#         material=db_fetchone('select * from inventory_list where inventory_code="%s"'%inv['inventory_code'])
#         if material['quantity']<(inv['quantity']*delta_item_counter):
#             return False
#     # for inv in req:
#     #     material=db_fetchone('select * from inventory_list where inventory_code="%s"'%inv['inventory_code'])
#     #     update_quantity=material['quantity']-inv['quantity']
#     #     sql='update inventory_list set quantity=%s where inventory_code=%s'%(update_quantity,inv['inventory_code'])
#     #     db_query(sql)
#     return True

# def substract_inventory(sku_code,delta_item_counter):
#     sku=db_fetchone('select * from sku_list where sku_code="%s"'%sku_code)
#     req=json.loads(sku['material'])
#     for inv in req:
#         # material=db_fetchone('select * from inventory_list where inventory_code="%s"'%inv['inventory_code'])
#         # if material['quantity']<inv['quantity']:
#         sql='update inventory_list set quantity=(quantity-%s) where inventory_code="%s"'%((inv['quantity']*delta_item_counter),inv['inventory_code'])
#         db_query(sql)

def oee():
    line=db_fetch('select * from manufacturing_line')
    for row in line:
        line_id=row['id']
        line_name=row['line_name']
        order_id=row['order_id']
        batch_id=row['batch_id']
        sku_code=row['sku_code']
        remark=row['remark']
        location=row['location']
        # performance=row['performance']
        # availability=row['availability']
        # quality=row['quality']
        status=row['status']
        temp_time=row['temp_time']
        setup_time=row['setup_time']
        cycle_time=row['cycle_time']
        run_time=row['run_time']
        down_time=row['down_time']
        standby_time=row['standby_time']
        ng_count=row['NG_count']
        item_counter=row['item_counter']
        prev_item_counter=row['prev_item_counter']
        acc_standby_time=row['acc_standby_time']
        acc_setup_time=row['acc_setup_time']
        order_id=row['order_id']
        target=row['target']
        sku_code=row['sku_code']
        if status!='STOP':
            if standby_time>0:
                standby_time-=1
                acc_standby_time+=1
                sql='update manufacturing_line set standby_time=%s,acc_standby_time=%s,status="STANDBY" where id=%s'%(standby_time,acc_standby_time,line_id)
                db_query(sql)
            else:
                if setup_time>0:
                    setup_time-=1
                    acc_setup_time+=1
                    sql='update manufacturing_line set setup_time=%s,acc_setup_time=%s,status="SETUP" where id=%s'%(setup_time,acc_setup_time,line_id)
                    db_query(sql)
                else:
                    if temp_time<cycle_time:
                        run_time+=1
                        sql='update manufacturing_line set run_time=%s,status="RUNNING",remark="",location="" where id=%s'%(run_time,line_id)
                        db_query(sql)
                    elif temp_time>=cycle_time:
                        down_time+=1
                        # sql='update manufacturing_line set down_time=%s,status="BREAKDOWN",remark="" where id=%s'%(down_time,line_id)
                        # db_query(sql)
                        if (temp_time-cycle_time) < 600:
                            sql='update manufacturing_line set down_time=%s,status="SMALL STOP",remark="" where id=%s'%(down_time,line_id)
                            db_query(sql)
                        elif (temp_time-cycle_time) >= 600:
                            #tinggal ganti breakdown ke Down Time
                            sql='update manufacturing_line set down_time=%s,status="DOWN TIME",remark="" where id=%s'%(down_time,line_id)
                            db_query(sql)
                    temp_time+=1
                    sql='update manufacturing_line set temp_time=%s where id=%s'%(temp_time,line_id)
                    db_query(sql)
            if item_counter!=prev_item_counter:
                temp_time=0
                delta_item_counter=item_counter-prev_item_counter
                prev_item_counter=item_counter
                sql='update manufacturing_line set temp_time=%s,prev_item_counter=%s,standby_time=0,setup_time=0,status="RUNNING" where id=%s'%(temp_time,prev_item_counter,line_id)
                db_query(sql)
        availability=round((run_time*100/(run_time+down_time)) if (run_time+down_time)!=0 else 0,2)
        performance=round(((cycle_time*item_counter)*100/(run_time+down_time)) if run_time!=0 else 0,2)
        quality=round(((item_counter-ng_count)*100/item_counter) if item_counter!=0 else 0,2)
        progress=round(((item_counter-ng_count)*100/target) if target!=0 else 0,2)
        sql='update manufacturing_line set performance=%s,availability=%s,quality=%s,progress=%s where id=%s'%(performance,availability,quality,progress,line_id)
        db_query(sql)
        prev_log = db_fetchone(
            'select * from log_oee where line_name="%s" order by timestamp desc limit 1' % line_name)
        prev_status = prev_log['status'] if prev_log else "STOP"
        if prev_status != status:
            # insert
            print('[MYSQL] Inserting log.')
            sql='insert into log_oee (order_id,batch_id,line_name,sku_code,item_counter,NG_count,status,performance,availability,quality,run_time,down_time,remark,acc_setup_time,acc_standby_time,location,prev_status) values(%s,"%s","%s","%s",%s,%s,"%s",%s,%s,%s,%s,%s,"%s",%s,%s,"%s","%s")'%(order_id,batch_id,line_name,sku_code,item_counter,ng_count,status,performance,availability,quality,run_time,down_time,remark,acc_setup_time,acc_standby_time,location,prev_status)
            db_query(sql)
        # sql='update order_list set performance=%s,availability=%s,quality=%s,progress=%s,NG_count=%s,item_counter=%s where id=%s'%(performance,availability,quality,progress,ng_count,item_counter,order_id)
        # db_query(sql)
        # print("setup time : %s"%row['setup_time'])
        # print("temp time : %s"%row['temp_time'])
        # print("run time : %s"%row['run_time'])
        # print("down time : %s"%row['down_time'])
        # print("standby time : %s"%row['standby_time'])
        # print("acc setup time : %s"%row['acc_setup_time'])
        # print("acc standby time : %s"%row['acc_standby_time'])
        # print('--------------')

def cronjob():
    line=db_fetch('select * from manufacturing_line')
    # prev_log=db_fetchone('SELECT * FROM log_oee ORDER BY timestamp DESC')
    for row in line:
        line_name=row['line_name']
        sku_code=row['sku_code']
        status=row['status']
        run_time=row['run_time']
        down_time=row['down_time']
        ng_count=row['NG_count']
        item_counter=row['item_counter']
        performance=row['performance']
        availability=row['availability']
        quality=row['quality']
        remark=row['remark']
        acc_standby_time=row['acc_standby_time']
        acc_setup_time=row['acc_setup_time']
        location=row['location']
        prev_log = db_fetchone(
            'select * from log_oee where line_name="%s" order by timestamp desc limit 1' % line_name)
        prev_status = prev_log['status'] if prev_log else "STOP"
        # prev_status=prev_log['status']
        sql='insert into log_oee (line_name,sku_code,item_counter,NG_count,status,performance,availability,quality,run_time,down_time,remark,acc_setup_time,acc_standby_time,location,prev_status) values("%s","%s",%s,%s,"%s",%s,%s,%s,%s,%s,"%s",%s,%s,"%s","%s")'%(line_name,sku_code,item_counter,ng_count,status,performance,availability,quality,run_time,down_time,remark,acc_setup_time,acc_standby_time,location,prev_status)
        db_query(sql)
        

try:
    db_connect('dbdemo.colinn.id','oee4','admin','adminiot',3307)
    # db_connect('localhost','oee4','root','iotdb123')
    previousTime = 0
    eventInterval = 1000
    schedule.every().minute.at(":00").do(cronjob)
    schedule.every(1).seconds.do(oee)
    # cronjob()
    while 1:
        if db_status():
            try:
                schedule.run_pending()
                # currentTime = millis()
                # if currentTime - previousTime >= eventInterval :
                #     oee()
                #     previousTime = currentTime
                time.sleep(1/1000)
            except KeyboardInterrupt:
                print('[PROGRAM] Closed')
                exit()
            except Exception as e: 
                print(e)
                time.sleep(1/1000000)
            except:
                time.sleep(1)
                print('[PROGRAM] Something is wrong')
                time.sleep(1/1000000)
        else:
            db_reconnect()

except KeyboardInterrupt:
    print('[PROGRAM] Closed')
    exit()
except Exception as e: 
    print(e)
    time.sleep(1/1000000)
except:
    print('[PROGRAM] Something is wrong')
    time.sleep(1/1000000)
finally:
    db_close()
