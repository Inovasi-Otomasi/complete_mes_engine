import time
import schedule
from db_func import *
import json


def millis():
    return int(time.time() * 1000)


def line_calculation(id):
    row = db_fetchone('select * from manufacturing_line where id=%s' % id)
    line_id = row['id']
    line_name = row['line_name']
    order_id = row['order_id']
    batch_id = row['batch_id']
    lot_number = row['lot_number']
    sku_code = row['sku_code']
    remark = row['remark']
    location = row['location']
    # performance=row['performance']
    # availability=row['availability']
    # quality=row['quality']
    status = row['status']
    temp_time = row['temp_time']
    setup_time = row['setup_time']
    cycle_time = row['cycle_time']
    run_time = row['run_time']
    down_time = row['down_time']
    standby_time = row['standby_time']
    ng_count = row['NG_count']
    prev_ng_count = row['prev_NG_count']
    acc_ng_count = row['acc_NG_count']
    item_counter = row['item_counter']
    prev_item_counter = row['prev_item_counter']
    acc_standby_time = row['acc_standby_time']
    acc_setup_time = row['acc_setup_time']
    acc_run_time = row['acc_run_time']
    acc_down_time = row['acc_down_time']
    acc_item_counter = row['acc_item_counter']
    acc_cycle_time = row['acc_cycle_time']
    small_stop_time = row['small_stop_time']
    # order_id=row['order_id']
    target = row['target']
    sku_code = row['sku_code']
    if status != 'STOP' and status != 'BREAKDOWN' and order_id != 0 and sku_code != 'None':
        if standby_time > 0:
            standby_time -= 1
            acc_standby_time += 1
            # if not standby_time ganti ke standby
            if status != 'STANDBY':
                sql = 'update manufacturing_line set standby_time=%s,acc_standby_time=%s,status="STANDBY" where id=%s' % (
                    standby_time, acc_standby_time, line_id)
                db_query(sql)
                status = 'STANDBY'
            else:
                sql = 'update manufacturing_line set standby_time=%s,acc_standby_time=%s where id=%s' % (
                    standby_time, acc_standby_time, line_id)
                db_query(sql)
        else:
            if setup_time > 0:
                setup_time -= 1
                acc_setup_time += 1
                if status != 'SETUP':
                    sql = 'update manufacturing_line set setup_time=%s,acc_setup_time=%s,status="SETUP" where id=%s' % (
                        setup_time, acc_setup_time, line_id)
                    db_query(sql)
                    status = 'SETUP'
                else:
                    sql = 'update manufacturing_line set setup_time=%s,acc_setup_time=%s where id=%s' % (
                        setup_time, acc_setup_time, line_id)
                    db_query(sql)
            else:
                if temp_time < cycle_time:
                    run_time += 1
                    acc_run_time += 1
                    if status != 'RUNNING':
                        sql = 'update manufacturing_line set run_time=%s,acc_run_time=%s,status="RUNNING",remark="",location="" where id=%s' % (
                            run_time, acc_run_time, line_id)
                        db_query(sql)
                        status = 'RUNNING'
                    else:
                        sql = 'update manufacturing_line set run_time=%s,acc_run_time=%s,remark="",location="" where id=%s' % (
                            run_time, acc_run_time, line_id)
                        db_query(sql)
                elif temp_time >= cycle_time:
                    down_time += 1
                    acc_down_time += 1
                    # sql='update manufacturing_line set down_time=%s,status="BREAKDOWN",remark="" where id=%s'%(down_time,line_id)
                    # db_query(sql)
                    if (temp_time - cycle_time) < small_stop_time:
                        if status != 'SMALL STOP':
                            sql = 'update manufacturing_line set down_time=%s,acc_down_time=%s,status="SMALL STOP",remark="" where id=%s' % (
                                down_time, acc_down_time, line_id)
                            db_query(sql)
                            status = 'SMALL STOP'
                        else:
                            sql = 'update manufacturing_line set down_time=%s,acc_down_time=%s,remark="" where id=%s' % (
                                down_time, acc_down_time, line_id)
                            db_query(sql)
                    elif (temp_time - cycle_time) >= small_stop_time:
                        # tinggal ganti breakdown ke Down Time
                        if status != 'DOWN TIME':
                            sql = 'update manufacturing_line set down_time=%s,acc_down_time=%s,status="DOWN TIME",remark="" where id=%s' % (
                                down_time, acc_down_time, line_id)
                            db_query(sql)
                            status = 'DOWN TIME'
                        else:
                            sql = 'update manufacturing_line set down_time=%s,acc_down_time=%s,remark="" where id=%s' % (
                                down_time, acc_down_time, line_id)
                            db_query(sql)
                temp_time += 1
                sql = 'update manufacturing_line set temp_time=%s where id=%s' % (temp_time, line_id)
                db_query(sql)
        if item_counter != prev_item_counter:
            temp_time = 0
            delta_item_counter = item_counter - prev_item_counter
            if item_counter >= prev_item_counter:
                acc_item_counter = delta_item_counter + acc_item_counter
                acc_cycle_time = (cycle_time * delta_item_counter) + acc_cycle_time
            prev_item_counter = item_counter
            if status != 'RUNNING':
                sql = 'update manufacturing_line set temp_time=%s,prev_item_counter=%s,acc_item_counter=%s,acc_cycle_time=%s,standby_time=0,setup_time=0,status="RUNNING" where id=%s' % (
                    temp_time, prev_item_counter, acc_item_counter, acc_cycle_time, line_id)
                db_query(sql)
                status = 'RUNNING'
            else:
                sql = 'update manufacturing_line set temp_time=%s,prev_item_counter=%s,acc_item_counter=%s,acc_cycle_time=%s,standby_time=0,setup_time=0 where id=%s' % (
                    temp_time, prev_item_counter, acc_item_counter, acc_cycle_time, line_id)
                db_query(sql)
        if ng_count != prev_ng_count:
            delta_ng_count = ng_count - prev_ng_count
            if ng_count >= prev_ng_count:
                acc_ng_count = delta_ng_count + acc_ng_count
            prev_ng_count = ng_count
            sql = 'update manufacturing_line set prev_NG_count=%s,acc_NG_count=%s where id=%s' % (
                prev_ng_count, acc_ng_count, line_id)
            db_query(sql)
    elif status == 'BREAKDOWN':
        # why status=breakdown?
        down_time += 1
        acc_down_time += 1
        sql = 'update manufacturing_line set down_time=%s,acc_down_time=%s,remark="" where id=%s' % (
            down_time, acc_down_time, line_id)
        db_query(sql)
    # disabled for status stop
    # else:
    #     sql = 'update manufacturing_line set status="STOP" where id=%s' % line_id
    #     db_query(sql)
    availability = round((run_time * 100 / (run_time + down_time)) if (run_time + down_time) != 0 else 0, 2)
    # availability_24h = round((acc_run_time * 100 / 86400), 2)
    availability_24h = round(((86400 - acc_down_time) * 100 / 86400), 2)
    # need acc_down_time
    performance = round(((cycle_time * item_counter) * 100 / (run_time + down_time)) if run_time != 0 else 0, 2)
    performance_24h = round(acc_cycle_time * 100 / 86400, 2)
    quality = round(((item_counter - ng_count) * 100 / item_counter) if item_counter != 0 else 0, 2)
    quality_24h = round(
        ((acc_item_counter - acc_ng_count) * 100 / acc_item_counter) if acc_item_counter != 0 else 0, 2)
    # avoid minus
    if quality < 0:
        quality = 0
    if quality_24h < 0:
        quality_24h = 0
    progress = round(((item_counter) * 100 / target) if target != 0 else 0, 2)
    sql = 'update manufacturing_line set performance=%s,performance_24h=%s,availability=%s,availability_24h=%s,quality=%s,quality_24h=%s,progress=%s where id=%s' % (
        performance, performance_24h, availability, availability_24h, quality, quality_24h, progress, line_id)
    db_query(sql)
    prev_log = db_fetchone(
        'select * from log_oee where line_name="%s" order by timestamp desc limit 1' % line_name)
    prev_status = prev_log['status'] if prev_log else "STOP"
    if prev_status != status:
        # insert
        print('[MYSQL] Inserting log.')
        sql = 'insert into log_oee (order_id,batch_id,lot_number,line_name,sku_code,item_counter,NG_count,status,performance,availability,quality,performance_24h,availability_24h,quality_24h,run_time,down_time,remark,acc_setup_time,acc_standby_time,location,prev_status,acc_item_counter,acc_NG_count,acc_run_time,acc_down_time) values(%s,"%s","%s","%s","%s",%s,%s,"%s",%s,%s,%s,%s,%s,%s,%s,%s,"%s",%s,%s,"%s","%s",%s,%s,%s,%s)' % (
            order_id, batch_id, lot_number, line_name, sku_code, item_counter, ng_count, status, performance,
            availability, quality, performance_24h, availability_24h, quality_24h, run_time, down_time, remark,
            acc_setup_time, acc_standby_time, location,
            prev_status, acc_item_counter, acc_ng_count, acc_run_time, acc_down_time)
        db_query(sql)
        if prev_status == 'SMALL STOP' and status == 'RUNNING':
            # get latest small stop from this line
            small_stop_log = db_fetchone(
                'select * from log_oee where line_name="%s" and status="SMALL STOP" and (prev_status="RUNNING" or prev_status="STOP" or prev_status="SETUP" or prev_status="STANDBY") order by timestamp desc limit 1' % line_name)
            delta_downtime = down_time - small_stop_log['down_time']
            print('calculating delta downtime')
            if small_stop_log:
                sql = 'update log_oee set delta_down_time=%s where id=%s' % (delta_downtime, small_stop_log['id'])
                db_query(sql)
                print('updating delta downtime')
            # selisih downtime
            # update selisih downtime to log
        elif prev_status == 'DOWN TIME' and status == 'RUNNING':
            # get latest small stop from this line
            small_stop_log = db_fetchone(
                'select * from log_oee where line_name="%s" and status="DOWN TIME" and (prev_status="SMALL STOP" or prev_status="STOP" or prev_status="RUNNING" or prev_status="SETUP" or prev_status="STANDBY") order by timestamp desc limit 1' % line_name)
            delta_downtime = down_time - small_stop_log['down_time']
            print('calculating delta downtime')
            if small_stop_log:
                sql = 'update log_oee set delta_down_time=%s where id=%s' % (delta_downtime, small_stop_log['id'])
                db_query(sql)
                print('updating delta downtime')
            # selisih downtime
            # update selisih downtime to log
        elif prev_status == 'SMALL STOP' and status == 'DOWN TIME':
            # get latest small stop from this line
            small_stop_log = db_fetchone(
                'select * from log_oee where line_name="%s" and status="SMALL STOP" and (prev_status="RUNNING" or prev_status="STOP" or prev_status="SETUP" or prev_status="STANDBY") order by timestamp desc limit 1' % line_name)
            delta_downtime = down_time - small_stop_log['down_time']
            print('calculating delta downtime')
            if small_stop_log:
                sql = 'update log_oee set delta_down_time=%s where id=%s' % (delta_downtime, small_stop_log['id'])
                db_query(sql)
                print('updating delta downtime')
            # selisih downtime
            # update selisih downtime to log
        elif prev_status == 'SMALL STOP' and status == 'STOP':
            # get latest small stop from this line
            small_stop_log = db_fetchone(
                'select * from log_oee where line_name="%s" and status="SMALL STOP" and (prev_status="RUNNING" or prev_status="STOP" or prev_status="SETUP" or prev_status="STANDBY") order by timestamp desc limit 1' % line_name)
            delta_downtime = down_time - small_stop_log['down_time']
            print('calculating delta downtime')
            if small_stop_log:
                sql = 'update log_oee set delta_down_time=%s where id=%s' % (delta_downtime, small_stop_log['id'])
                db_query(sql)
                print('updating delta downtime')
                sql = 'update manufacturing_line set down_time=0 where id=%s' % line_id
                db_query(sql)
            # selisih downtime
            # update selisih downtime to log
        elif prev_status == 'DOWN TIME' and status == 'STOP':
            # get latest small stop from this line
            small_stop_log = db_fetchone(
                'select * from log_oee where line_name="%s" and status="DOWN TIME" and (prev_status="SMALL STOP" or prev_status="STOP" or prev_status="RUNNING" or prev_status="SETUP" or prev_status="STANDBY") order by timestamp desc limit 1' % line_name)
            delta_downtime = down_time - small_stop_log['down_time']
            print('calculating delta downtime')
            if small_stop_log:
                sql = 'update log_oee set delta_down_time=%s where id=%s' % (delta_downtime, small_stop_log['id'])
                db_query(sql)
                print('updating delta downtime')
                sql = 'update manufacturing_line set down_time=0 where id=%s' % line_id
                db_query(sql)
            # selisih downtime
            # update selisih downtime to log
        elif prev_status == 'SMALL STOP' and status == 'STANDBY':
            # get latest small stop from this line
            small_stop_log = db_fetchone(
                'select * from log_oee where line_name="%s" and status="SMALL STOP" and (prev_status="RUNNING" or prev_status="STOP" or prev_status="SETUP" or prev_status="STANDBY") order by timestamp desc limit 1' % line_name)
            delta_downtime = down_time - small_stop_log['down_time']
            print('calculating delta downtime')
            if small_stop_log:
                sql = 'update log_oee set delta_down_time=%s where id=%s' % (delta_downtime, small_stop_log['id'])
                db_query(sql)
                print('updating delta downtime')
            # selisih downtime
            # update selisih downtime to log
        elif prev_status == 'DOWN TIME' and status == 'STANDBY':
            # get latest small stop from this line
            small_stop_log = db_fetchone(
                'select * from log_oee where line_name="%s" and status="DOWN TIME" and (prev_status="SMALL STOP" or prev_status="STOP" or prev_status="RUNNING" or prev_status="SETUP" or prev_status="STANDBY") order by timestamp desc limit 1' % line_name)
            delta_downtime = down_time - small_stop_log['down_time']
            print('calculating delta downtime')
            if small_stop_log:
                sql = 'update log_oee set delta_down_time=%s where id=%s' % (delta_downtime, small_stop_log['id'])
                db_query(sql)
                print('updating delta downtime')
            # selisih downtime
            # update selisih downtime to log
        elif prev_status == 'BREAKDOWN' and status == 'STOP':
            # get latest small stop from this line
            small_stop_log = db_fetchone(
                'select * from log_oee where line_name="%s" and status="BREAKDOWN" and prev_status="STOP" order by timestamp desc limit 1' % line_name)
            delta_downtime = down_time - small_stop_log['down_time']
            print('calculating delta downtime')
            if small_stop_log:
                sql = 'update log_oee set delta_down_time=%s where id=%s' % (delta_downtime, small_stop_log['id'])
                db_query(sql)
                print('updating delta downtime')
                sql = 'update manufacturing_line set down_time=0 where id=%s' % line_id
                db_query(sql)
            # selisih downtime
            # update selisih downtime to log
        elif (prev_status == 'RUNNING' or prev_status == 'SETUP' or prev_status == 'STANDBY') and status == 'STOP':
            sql = 'update manufacturing_line set down_time=0,run_time=0 where id=%s' % line_id
            db_query(sql)
    print("setup time : %s" % row['setup_time'])
    print("temp time : %s" % row['temp_time'])
    print("run time : %s" % row['run_time'])
    print("down time : %s" % row['down_time'])
    print("standby time : %s" % row['standby_time'])
    print("acc setup time : %s" % row['acc_setup_time'])
    print("acc standby time : %s" % row['acc_standby_time'])
    print('--------------')


def oee():
    line = db_fetch('select * from manufacturing_line')
    for row in line:
        line_id = row['id']
        line_calculation(line_id)

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
    line = db_fetch('select * from manufacturing_line')
    # prev_log=db_fetchone('SELECT * FROM log_oee ORDER BY timestamp DESC')
    for row in line:
        line_name = row['line_name']
        order_id = row['order_id']
        batch_id = row['batch_id']
        lot_number = row['lot_number']
        sku_code = row['sku_code']
        status = row['status']
        run_time = row['run_time']
        down_time = row['down_time']
        ng_count = row['NG_count']
        item_counter = row['item_counter']
        performance = row['performance']
        availability = row['availability']
        quality = row['quality']
        performance_24h = row['performance_24h']
        availability_24h = row['availability_24h']
        quality_24h = row['quality_24h']
        remark = row['remark']
        acc_standby_time = row['acc_standby_time']
        acc_setup_time = row['acc_setup_time']
        acc_item_counter = row['acc_item_counter']
        acc_ng_count = row['acc_NG_count']
        acc_run_time = row['acc_run_time']
        acc_down_time = row['acc_down_time']
        location = row['location']
        prev_log = db_fetchone(
            'select * from log_oee where line_name="%s" order by timestamp desc limit 1' % line_name)
        prev_status = prev_log['status'] if prev_log else "STOP"
        # prev_status=prev_log['status']
        sql = 'insert into log_oee (order_id,batch_id,lot_number,line_name,sku_code,item_counter,NG_count,status,performance,availability,quality,performance_24h, availability_24h, quality_24h,run_time,down_time,remark,acc_setup_time,acc_standby_time,location,prev_status,acc_item_counter,acc_NG_count,acc_run_time,acc_down_time) values(%s,"%s","%s","%s","%s",%s,%s,"%s",%s,%s,%s,%s,%s,%s,%s,%s,"%s",%s,%s,"%s","%s",%s,%s,%s,%s)' % (
            order_id, batch_id, lot_number, line_name, sku_code, item_counter, ng_count, status, performance,
            availability, quality, performance_24h, availability_24h, quality_24h, run_time, down_time, remark,
            acc_setup_time, acc_standby_time, location, prev_status, acc_item_counter, acc_ng_count, acc_run_time,
            acc_down_time)
        db_query(sql)


def reset_oee_24h():
    sql = 'update manufacturing_line set acc_down_time=0,acc_run_time=0,acc_item_counter=0,acc_cycle_time=0,acc_NG_count=0,performance_24h=0,availability_24h=0,quality_24h=0'
    db_query(sql)


try:
    db_connect('172.17.0.1', 'oee4', 'admin', 'adminiot', 33069)
    # db_connect('localhost','oee4','root','iotdb123')
    # db_connect('localhost', 'oee4', 'admin', 'adminiot')
    previousTime = 0
    eventInterval = 1000
    schedule.every().day.at("00:00").do(reset_oee_24h)
    schedule.every().minute.at(":00").do(cronjob)
    # schedule.every(1).seconds.do(oee)
    # cronjob()
    while 1:
        if db_status():
            try:
                schedule.run_pending()
                currentTime = millis()
                if currentTime - previousTime >= eventInterval:
                    print('=============')
                    print('loop time: %s' % (currentTime - previousTime))
                    print('=============')
                    oee()
                    previousTime = currentTime
                time.sleep(1 / 1000)
            except KeyboardInterrupt:
                print('[PROGRAM] Closed')
                exit()
            except Exception as e:
                print(e)
                time.sleep(1 / 1000000)
            except:
                time.sleep(1)
                print('[PROGRAM] Something is wrong')
                time.sleep(1 / 1000000)
        else:
            db_reconnect()

except KeyboardInterrupt:
    print('[PROGRAM] Closed')
    exit()
except Exception as e:
    print(e)
    time.sleep(1 / 1000000)
except:
    print('[PROGRAM] Something is wrong')
    time.sleep(1 / 1000000)
finally:
    db_close()
