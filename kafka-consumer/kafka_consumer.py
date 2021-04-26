from kafka import KafkaConsumer
import time, json, uuid, pymysql # mariadb import 제거
import threading
from datetime import datetime

# 수정한 부분 - bootstrap_servers 의 주소를 localhost 에서 kafka-docker의 IPv4 주소로 변경
consumer = KafkaConsumer('my_topic_users',
                        #  bootstrap_servers=["172.20.0.101:9092"],
                         bootstrap_servers=["127.0.0.1:9092"],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         auto_commit_interval_ms=1000,
                         consumer_timeout_ms=1000)

consumer2 = KafkaConsumer('my_topic_festival',
                         #  bootstrap_servers=["172.20.0.101:9092"],
                         bootstrap_servers=["127.0.0.1:9092"],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         auto_commit_interval_ms=1000,
                         consumer_timeout_ms=1000)

consumer3 = KafkaConsumer('my_topic_store',
                         #  bootstrap_servers=["172.20.0.101:9092"],
                         bootstrap_servers=["127.0.0.1:9092"],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         auto_commit_interval_ms=1000,
                         consumer_timeout_ms=1000)

consumer4 = KafkaConsumer('my_topic_menu',
                         #  bootstrap_servers=["172.20.0.101:9092"],
                         bootstrap_servers=["127.0.0.1:9092"],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         auto_commit_interval_ms=1000,
                         consumer_timeout_ms=1000)

consumer5 = KafkaConsumer('my_topic_orders',
                         #  bootstrap_servers=["172.20.0.101:9092"],
                         bootstrap_servers=["127.0.0.1:9092"],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         auto_commit_interval_ms=1000,
                         consumer_timeout_ms=1000)

consumer6 = KafkaConsumer('my_topic_order_detail',
                         #  bootstrap_servers=["172.20.0.101:9092"],
                         bootstrap_servers=["127.0.0.1:9092"],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         auto_commit_interval_ms=1000,
                         consumer_timeout_ms=1000)


config = {
    'host': '127.0.0.1', # 수정한 부분 - 내 mysql 의 IPv4 주소로 변경
    'port': 23306,
    'user': 'root',
    'database': 'mydb',
    'charset': 'utf8'
}


conn = pymysql.connect(**config) # mariadb에서 pymysql로 변경
cursor = conn.cursor()


def fetch_latest_orders(next_call_in):
    # 10초에 한번씩 가져오기
    next_call_in += 10

    # consumer들의 데이터 읽어오는 함수 poll
    # users
    batch = consumer.poll(timeout_ms=100)
    if len(batch) > 0:
        for message in list(batch.values())[0]:
            value = message.value.decode()
            value_dict = json.loads(value)

            # users데이터가 있는지 없는지 확인
            sql_s = '''SELECT * FROM users'''
            cursor.execute(sql)
            check = cursor.fetchall()

            for ch in check:
                if ch['user_no'] == value_dict['user_no']:
                    sql = '''UPDATE users SET phone_number=%s WHERE user_no=%s'''
                    cursor.execute(sql, [value_dict['phone_number'], value_dict['user_no']])
                    conn.commit()
                    print('users 데이터 입력됨')
                    
                else:
                    # DB insert
                    sql = '''INSERT INTO users(user_category, email, user_name)
                            VALUES(%s,%s,%s)'''
                    cursor.execute(sql, [value_dict['user_category'], value_dict['email'], value_dict['user_name']])
                    conn.commit()
                    print('users 데이터 입력됨')
                    



            # 데이터 삭제 여부 검사 후 삭제
            # cursor.execute("SELECT * FROM users")
            # delete_check = cursor.fetchall()
            #     for i in delete_check:
            #         if i not in value_dict:
            #             cursor.execute("DELETE FROM users WHERE user_category=%s AND email=%s AND user_name=%s AND phone_number=%s",
            #                             [i[1], i[2], i[3], i[4]])
            #             print(f'{i[1]}, {i[2]}, {i[3]}, {i[4]} users 삭제됨')
    

    # festival
    batch2 = consumer2.poll(timeout_ms=100)
    if len(batch2) > 0:
        for message in list(batch2.values())[0]:
            value = message.value.decode()
            value_dict = json.loads(value)
            
            # DB insert
            sql = '''INSERT INTO festival(user_no, company_name, festival_name, period, location, url)
                        VALUES(%s,%s,%s,%s,%s,%s)'''
            # 데이터 중복 여부 검사
            cursor.execute(sql, (value_dict['user_no'], value_dict['company_name'], 
                                value_dict['company_name'], value_dict['festival_name'], 
                                value_dict['period'], value_dict['location'], value_dict['url']))
            
            conn.commit()
            print('festival 데이터 입력됨')


            
    #         # 데이터 삭제 여부 검사 후 삭제
    #         cursor.execute("SELECT * FROM festival")
    #         delete_check = cursor.fetchall()
    #         if len(value_dict) < len(delete_check):
    #             for i in delete_check:
    #                 if i not in value_dict:
    #                     cursor.execute("DELETE FROM festival WHERE user_no=%s AND company_name=%s AND festival_name=%s AND period=%s AND location=%s AND url=%s",
    #                                     [i[1], i[2], i[3], i[4], i[5], i[6]])
    #                     print(f'{i[1]}, {i[2]}, {i[3]}, {i[4]}, {i[5]} {i[6]} festival 삭제됨')


    # store
    batch3 = consumer3.poll(timeout_ms=100)
    if len(batch3) > 0:
        for message in list(batch3.values())[0]:
            value = message.value.decode()
            value_dict = json.loads(value)
            
            # DB insert
            sql = '''INSERT INTO store(user_no, festival_id, store_name, store_description,
                             contact_number, category, license_number, location_number)
#                               VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'''

            cursor.execute(sql, (value_dict[i][1], value_dict[i][2], 
                                value_dict[i][3], value_dict[i][4], 
                                value_dict[i][5], value_dict[i][6], 
                                value_dict[i][7], value_dict[i][8],))

            conn.commit()
            print('store 데이터 입력됨')
            



    #         # 데이터 삭제 여부 검사 후 삭제
    #         cursor.execute("SELECT * FROM store")
    #         delete_check = cursor.fetchall()
    #         if len(value_dict) < len(delete_check):
    #             for i in delete_check:
    #                 if i not in value_dict:
    #                     cursor.execute("DELETE FROM store WHERE user_no=%s AND festival_id=%s AND store_name=%s AND store_description=%s AND contact_number=%s AND category=%s AND license_number=%s AND location_number=%s",
    #                                     [i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8]])
    #                     print(f'{i[1]}, {i[2]}, {i[3]}, {i[4]}, {i[5]}, {i[6]}, {i[7]}, {i[8]} store 삭제됨')


    # menu
    batch4 = consumer4.poll(timeout_ms=100)
    if len(batch4) > 0:
        for message in list(batch4.values())[0]:
            value = message.value.decode()
            value_dict = json.loads(value)
            
            # DB insert
            sql = '''INSERT INTO menu(store_id, menu_name, menu_price)
                        VALUES(%s,%s,%s)'''
            cursor.execute(sql, (value_dict['store_id'], value_dict['menu_name'], value_dict['menu_price']))

            conn.commit()
            print('menu 데이터 입력됨')
            


    #         # 데이터 삭제 여부 검사 후 삭제
    #         cursor.execute("SELECT * FROM menu")
    #         delete_check = cursor.fetchall()
    #         if len(value_dict) < len(delete_check):
    #             for i in delete_check:
    #                 if i not in value_dict:
    #                     cursor.execute("DELETE FROM menu WHERE store_id=%s AND menu_name=%s AND menu_price=%s",
    #                                     [i[1], i[2], i[3]])
    #                     print(f'{i[1]}, {i[2]}, {i[3]} menu 삭제됨')
    
            

    # orders
    batch5 = consumer5.poll(timeout_ms=100)
    if len(batch5) > 0:
        for message in list(batch5.values())[0]:
            value = message.value.decode()
            value_dict = json.loads(value)
            
            # DB insert
            sql = '''INSERT INTO orders(user_no, store_id, total_qty, total_price)
                        VALUES(%s,%s,%s,%s,%s)'''
            cursor.execute(sql, (value_dict['user_no'], value_dict['store_id'], 
                                value_dict['total_qty'], value_dict['total_price']))
            
            conn.commit()
            print('orders 데이터 입력됨')




    #         # 데이터 삭제 여부 검사 후 삭제
    #         cursor.execute("SELECT * FROM orders")
    #         delete_check = cursor.fetchall()
    #         if len(value_dict) < len(delete_check):
    #             for i in delete_check:
    #                 if i not in value_dict:
    #                     cursor.execute("DELETE FROM orders WHERE user_no=%s AND store_id=%s AND total_qty=%s AND total_price=%s AND requests=%s",
    #                                     [i[1], i[2], i[3], i[4], i[5]])
    #                     print(f'{i[1]}, {i[2]}, {i[3]}, {i[4]}, {i[5]} orders 삭제됨')
            

    # # order_detail
    batch6 = consumer6.poll(timeout_ms=100)
    if len(batch6) > 0:
        for message in list(batch6.values())[0]:
            value = message.value.decode()
            value_dict = json.loads(value)
            
            # DB insert
            sql = '''INSERT INTO order_detail(order_id, menu_id, food_price, food_qty)
                        VALUES(%s,%s,%s,%s)'''
            cursor.execute(sql, (value_dict['order_id'], value_dict['menu_id'], 
                                value_dict['food_price'], value_dict['food_qty']))


          
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