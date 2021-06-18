from kafka import KafkaConsumer
import time, json, uuid, pymysql # mariadb import 제거
import threading
from datetime import datetime

# 수정한 부분 - bootstrap_servers 의 주소를 localhost 에서 kafka-docker의 IPv4 주소로 변경
consumer = KafkaConsumer('my_topic_users',
                        bootstrap_servers=["172.20.0.101:9092"],
                         # bootstrap_servers=["127.0.0.1:9092"],
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        auto_commit_interval_ms=1000,
                        consumer_timeout_ms=3000)

consumer2 = KafkaConsumer('my_topic_festival',
                        bootstrap_servers=["172.20.0.101:9092"],
                        # bootstrap_servers=["127.0.0.1:9092"],
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        auto_commit_interval_ms=1000,
                        consumer_timeout_ms=3000)

consumer3 = KafkaConsumer('my_topic_store',
                        bootstrap_servers=["172.20.0.101:9092"],
                        # bootstrap_servers=["127.0.0.1:9092"],
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        auto_commit_interval_ms=1000,
                        consumer_timeout_ms=3000)

consumer4 = KafkaConsumer('my_topic_menu',
                        bootstrap_servers=["172.20.0.101:9092"],
                        # bootstrap_servers=["127.0.0.1:9092"],
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        auto_commit_interval_ms=1000,
                        consumer_timeout_ms=3000)

consumer5 = KafkaConsumer('my_topic_orders',
                        bootstrap_servers=["172.20.0.101:9092"],
                        # bootstrap_servers=["127.0.0.1:9092"],
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        auto_commit_interval_ms=1000,
                        consumer_timeout_ms=3000)

consumer6 = KafkaConsumer('my_topic_order_detail',
                        bootstrap_servers=["172.20.0.101:9092"],
                        #  bootstrap_servers=["127.0.0.1:9092"],
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        auto_commit_interval_ms=1000,
                        consumer_timeout_ms=3000)


config = {
    'host': '172.20.0.3', # 수정한 부분 - 내 mysql 의 IPv4 주소로 변경
    'port': 3306,
    'user': 'root',
    'database': 'mydb',
    'charset': 'utf8'
}


conn = pymysql.connect(**config) # mariadb에서 pymysql로 변경
cursor = conn.cursor()


def fetch_latest_orders(next_call_in):
    # 10초에 한번씩 가져오기
    next_call_in += 10

    
    ############## users ##############
    # consumer들의 데이터 읽어오는 함수 poll
    batch = consumer.poll(timeout_ms=100)
    if len(batch) > 0:
        for message in reversed(list(batch.values())[0]):
            value = message.value.decode()
            value_dict = json.loads(value)

            # users 데이터가 있는지 없는지 확인
            sql_s = '''SELECT * FROM users'''
            cursor.execute(sql_s)
            check = cursor.fetchall()
            print(value_dict)
            # apiDB에 값이 없으면
            if len(check) == 0:
                # DB insert
                sql = '''INSERT INTO users(user_category, email, user_name)
                        VALUES(%s,%s,%s)'''
                cursor.execute(sql, [value_dict['user_category'], value_dict['email'], value_dict['user_name']])
                conn.commit()
                print('users 데이터 입력됨')

                break

            # apiDB에 값이 한개 있으면
            elif len(check) == 1:
                # user_category에 null 값이면
                if value_dict['phone_number'] is None:
                    # apiDB의 user_no와 Kafka Server의 user_no가 같으면
                    if value_dict['user_no'] == check[0]:
                        pass
                    
                    # apiDB의 user_no와 Kafka Server의 user_no가 다르면
                    else:
                        # DB insert
                        sql = '''INSERT INTO users(user_category, email, user_name)
                                VALUES(%s,%s,%s)'''
                        cursor.execute(sql, [value_dict['user_category'], value_dict['email'], value_dict['user_name']])
                        
                        conn.commit()
                        print('users 데이터 입력됨')

                        break

                # phone_number에 값이 있으면
                else:
                    # apiDB의 user_no와 Kafka Server의 user_no가 같으면
                    if value_dict['user_no'] == check[0]:     
                        sql = '''UPDATE users SET phone_number=%s WHERE user_no=%s'''
                        cursor.execute(sql, [value_dict['phone_number'], value_dict['user_no']])
                        conn.commit()
                        
                        print('users 데이터 입력됨')

                        break

                    else:
                        pass

            # apiDB에 값이 2개 이상이면
            else:       
                # apiDB 한개씩 추출
                print(value_dict)
                for i in check:
                    # phone_number에 null 값이면
                    if value_dict['phone_number'] is None: 
                        # apiDB의 user_no와 Kafka Server의 user_no가 같으면
                        if value_dict['user_no'] == i[0]:     

                            pass
                        
                        else:
                            # DB insert
                            sql = '''INSERT INTO users(user_category, email, user_name)
                                    VALUES(%s,%s,%s)'''
                            cursor.execute(sql, [value_dict['user_category'], value_dict['email'], value_dict['user_name']])
                            
                            conn.commit()
                            print('users 데이터 입력됨')

                            break

                    else:
                        if value_dict['user_no'] == i[0]:
                            sql = '''UPDATE users SET phone_number=%s WHERE user_no=%s'''
                            cursor.execute(sql, [value_dict['phone_number'], value_dict['user_no']])
                            conn.commit()
                            
                            print('users 데이터 입력됨')

                            break
                        
                        else:
                            pass


    ############## festival ##############
    batch2 = consumer2.poll(timeout_ms=100)
    if len(batch2) > 0:
        for message in reversed(list(batch2.values())[0]):
            value = message.value.decode()
            value_dict = json.loads(value)

            # festival 데이터가 있는지 없는지 확인
            sql_s = '''SELECT * FROM festival'''
            cursor.execute(sql_s)
            check = cursor.fetchall()
            
            if len(check) == 0:
                # DB insert
                sql = '''INSERT INTO festival(user_no, company_name, festival_name, period, location, url)
                            VALUES(%s,%s,%s,%s,%s,%s)'''

                cursor.execute(sql, [value_dict['user_no'], value_dict['company_name'], value_dict['festival_name'], 
                                    value_dict['period'], value_dict['location'], value_dict['url']])
                
                conn.commit()

                print('festival 데이터 입력됨')

                break
            
            elif len(check) == 1:
                if value_dict['festival_id'] != check[0]:
                    sql = '''INSERT INTO festival(user_no, company_name, festival_name, period, location, url)
                                VALUES(%s,%s,%s,%s,%s,%s)'''

                    cursor.execute(sql, [value_dict['user_no'], value_dict['company_name'], value_dict['festival_name'], 
                                        value_dict['period'], value_dict['location'], value_dict['url']])
                    
                    conn.commit()
                    
                    print('festival 데이터 입력됨')

                    break
                
                else:
                    pass
            
            else:
                flag = 0
                for i in check:
                    if value_dict['festival_id'] != i[0]:
                        flag += 1

                    else:
                        pass
                
                if flag > 0:
                    # DB insert
                    sql = '''INSERT INTO festival(user_no, company_name, festival_name, period, location, url)
                                VALUES(%s,%s,%s,%s,%s,%s)'''

                    cursor.execute(sql, [value_dict['user_no'], value_dict['company_name'], value_dict['festival_name'], 
                                        value_dict['period'], value_dict['location'], value_dict['url']])
                    
                    conn.commit()
                    print('festival 데이터 입력됨')
                


    ############## store ##############
    batch3 = consumer3.poll(timeout_ms=100)
    if len(batch3) > 0:
        for message in reversed(list(batch3.values())[0]):
            value = message.value.decode()
            value_dict = json.loads(value)

            # store 데이터가 있는지 없는지 확인
            sql_s = '''SELECT * FROM store'''
            cursor.execute(sql_s)
            check = cursor.fetchall()
            
            if len(check) == 0:
                # DB insert
                sql = '''INSERT INTO store(user_no, festival_id, store_name, store_description, contact_number,
                        category, license_number, location_number) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''
                
                cursor.execute(sql, [value_dict['user_no'], value_dict['festival_id'], value_dict['store_name'],
                                    value_dict['store_description'], value_dict['contact_number'], value_dict['category'],
                                    value_dict['license_number'], value_dict['location_number']])
                conn.commit()
                print('store 데이터 입력됨')

                break
            
            elif len(check) == 1:
                if value_dict['store_id'] != check[0]:
                    sql = '''INSERT INTO store(user_no, festival_id, store_name, store_description, contact_number,
                        category, license_number, location_number) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''
                
                    cursor.execute(sql, [value_dict['user_no'], value_dict['festival_id'], value_dict['store_name'],
                                        value_dict['store_description'], value_dict['contact_number'], value_dict['category'],
                                        value_dict['license_number'], value_dict['location_number']])
                    conn.commit()
                    print('store 데이터 입력됨')

                    break
                
                else:
                    pass
            
            else:
                flag = 0
                for i in check:
                    if value_dict['store_id'] != i[0]:
                        flag += 1

                    else:
                        pass
                
                if flag > 0:
                    # DB insert
                    sql = '''INSERT INTO store(user_no, festival_id, store_name, store_description, contact_number,
                        category, license_number, location_number) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''
                
                    cursor.execute(sql, [value_dict['user_no'], value_dict['festival_id'], value_dict['store_name'],
                                        value_dict['store_description'], value_dict['contact_number'], value_dict['category'],
                                        value_dict['license_number'], value_dict['location_number']])
                    conn.commit()
                    print('store 데이터 입력됨')


    ############## menu ##############
    batch4 = consumer4.poll(timeout_ms=100)
    if len(batch4) > 0:
        for message in reversed(list(batch4.values())[0]):
            value = message.value.decode()
            value_dict = json.loads(value)

            # menu 데이터가 있는지 없는지 확인
            sql_s = '''SELECT * FROM menu'''
            cursor.execute(sql_s)
            check = cursor.fetchall()
            
            if len(check) == 0:
                # DB insert
                sql = '''INSERT INTO menu(store_id, menu_name, menu_price)
                        VALUES(%s,%s,%s)'''

                cursor.execute(sql, [value_dict['store_id'], value_dict['menu_name'], value_dict['menu_price']])
                
                conn.commit()

                print('menu 데이터 입력됨')

                break
            
            elif len(check) == 1:
                if value_dict['menu_id'] != check[0]:
                    sql = '''INSERT INTO menu(store_id, menu_name, menu_price)
                        VALUES(%s,%s,%s)'''

                    cursor.execute(sql, [value_dict['store_id'], value_dict['menu_name'], value_dict['menu_price']])
                
                    conn.commit()
                    
                    print('menu 데이터 입력됨')

                    break
                
                else:
                    pass
            
            else:
                flag = 0
                for i in check:
                    if value_dict['menu_id'] != i[0]:
                        flag += 1

                    else:
                        pass
                
                if flag > 0:
                    # DB insert
                    sql = '''INSERT INTO menu(store_id, menu_name, menu_price)
                        VALUES(%s,%s,%s)'''

                    cursor.execute(sql, [value_dict['store_id'], value_dict['menu_name'], value_dict['menu_price']])
                
                    conn.commit()
                    print('menu 데이터 입력됨')
              




    ############## orders ##############
    # batch5 = consumer5.poll(timeout_ms=100)
    # if len(batch5) > 0:
    #     for message in reversed(list(batch5.values())[0]):
    #         print("message :", message)
    #         value = message.value.decode()
    #         value_dict = json.loads(value)

    #         # orders 데이터가 있는지 없는지 확인
    #         sql_s = '''SELECT * FROM orders'''
    #         cursor.execute(sql_s)
    #         check = cursor.fetchall()
            
    #     # DB insert
    #     if len(check) == 0:
    #         sql = '''INSERT INTO orders(user_no, store_id, total_qty, total_price)
    #                     VALUES(%s,%s,%s,%s,%s)'''

    #         cursor.execute(
    #             sql, [value_dict['user_no'], value_dict['store_id'],
    #                 value_dict['total_qty'], value_dict['total_price']])

    #         conn.commit()
    #         print('orders 데이터 입력됨')

    #     elif len(check) == 1:
    #         if value_dict['requests'] == null:
    #             if value_dict['order_id'] == check[0]:
    #                 sql = "UPDATE orders SET requests=%s WHERE order_id=%s"
    #                 cursor.execute(sql, [value_dict['request_text'], value_dict['order_id']])
                    
    #                 conn.commit()
    #                 print('orders 데이터 입력됨')

    #             else:         
    #                 sql = '''INSERT INTO orders(user_no, store_id, total_qty, total_price)
    #                             VALUES(%s,%s,%s,%s,%s)'''

    #                 cursor.execute(
    #                     sql, [value_dict['user_no'], value_dict['store_id'],
    #                         value_dict['total_qty'], value_dict['total_price']])

    #                 conn.commit()
    #                 print('orders 데이터 입력됨')

    #         else:
    #             sql = "UPDATE orders SET requests=%s WHERE order_id=%s"
    #             cursor.execute(sql, [value_dict['request_text'], value_dict['order_id']])
                
    #             conn.commit()
    #             print('orders 데이터 입력됨')

    #     else:
    #         flag = 0       
    #         for i in check:
    #             if value_dict['requests'] == null:
    #                 if value_dict['order_id'] == check[0]:
    #                     sql = '''UPDATE orders SET phone_number=%s WHERE order_id=%s'''
    #                     cursor.execute(sql, [value_dict['phone_number'], value_dict['order_id']])

    #                     conn.commit()
    #                     print('orders 데이터 입력됨')

    #                 else:
    #                     flag += 1
                
    #             else:
                    
            
    #         if flag == 0:
    #             sql = '''INSERT INTO orders(user_no, store_id, total_qty, total_price)
    #                         VALUES(%s,%s,%s,%s,%s)'''
    #             cursor.execute(
    #                 sql, (value_dict['user_no'], value_dict['store_id'],
    #                     value_dict['total_qty'], value_dict['total_price']))

    #             conn.commit()
    #             print('orders 데이터 입력됨')
         

    ############## order_detail ##############
    # batch6 = consumer6.poll(timeout_ms=100)
    # if len(batch6) > 0:
    #     for message in reversed(list(batch6.values())[0]):
    #         value = message.value.decode()
    #         value_dict = json.loads(value)

    #         # order_detail데이터가 있는지 없는지 확인
    #         sql_s = '''SELECT * FROM order_detail'''
    #         cursor.execute(sql_s)
    #         check = cursor.fetchall()
            
    #         if len(check) == 0:
    #         # DB insert
    #             sql = '''INSERT INTO order_detail(order_id, menu_id, food_price, food_qty)
    #                         VALUES(%s,%s,%s,%s)'''
    #             cursor.execute(sql,
    #                         (value_dict['order_id'], value_dict['menu_id'],
    #                             value_dict['food_price'], value_dict['food_qty']))
    #             conn.commit()

    #             print('order_detail 데이터 입력됨')                              

    #         elif len(check) == 1:
    #             if value_dict['order_detail_id'] != check[i][0]:
    #                 sql = '''INSERT INTO order_detail(order_id, menu_id, food_price, food_qty)
    #                             VALUES(%s,%s,%s,%s)'''
    #                 cursor.execute(sql,
    #                             (value_dict['order_id'], value_dict['menu_id'],
    #                                 value_dict['food_price'], value_dict['food_qty']))
    #                 conn.commit()
                    
    #                 print('order_detail 데이터 입력됨')    
                
    #             else:
    #                 pass

    #         else:
    #             flag = 0
    #             for i in check:
    #                 if value_dict['order_detail_id'] != check[i][0]:
    #                     flag += 1

    #                 else:
    #                     pass
                
    #             if flag > 0:
    #                 # DB insert
    #                 sql = '''INSERT INTO order_detail(order_id, menu_id, food_price, food_qty)
    #                             VALUES(%s,%s,%s,%s)'''
    #                 cursor.execute(sql,
    #                             (value_dict['order_id'], value_dict['menu_id'],
    #                                 value_dict['food_price'], value_dict['food_qty']))
    #                 conn.commit()
    #                 print('order_detail 데이터 입력됨')


          
    #         # 데이터 삭제 여부 검사 후 삭제
    #         cursor.execute("SELECT * FROM order_detail")
    #         delete_check = cursor.fetchall()
    #         print('order_detail data 삭제 검사: ', delete_check, type(delete_check))
    #         if len(value_dict) < len(delete_check):
    #             for i in delete_check:
    #                 if i not in value_dict:
    #                     cursor.execute("DELETE FROM order_detail WHERE order_id=%s AND menu_id=%s AND food_price=%s AND food_qty=%s",
    #                                     [i[1], i[2], i[3], i[4]])
    #                     print(f'{i[1]}, {i[2]}, {i[3]}, {i[4]}, order_detail 삭제됨')

    threading.Timer(next_call_in - time.time(), fetch_latest_orders, [next_call_in]).start()
    
next_call_in = time.time()
fetch_latest_orders(next_call_in)