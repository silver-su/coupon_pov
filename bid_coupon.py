common_variable.py                                                                                  000644  000765  000024  00000001213 14516645031 015606  0                                                                                                    ustar 00silversu                        staff                           000000  000000                                                                                                                                                                         from pymongo import MongoClient, WriteConcern
from datetime import datetime

MONGODB_URI = "mongodb+srv://91app:nntO5Hx4Ox6C0JJZ@coupon.0pfa3.mongodb.net/?retryWrites=true&w=majority&maxPoolSize=2000&readPreference=secondary"
MONGODB_DB = "91app"
COLL_COUPON = "coupon"
COLL_COUPON_SLAVE = "coupon_slave"
COLL_MEMBER = "member"
COLL_LOCUST_MEMBER = "locust_member"
COLL_LOG = "log"
COLL_COUPON_SUMMARY = "coupon_summary"

def logging(conn: MongoClient, case: str, action: str):
    conn[MONGODB_DB][COLL_LOG].with_options(write_concern=WriteConcern(w=1)).insert_one({
        "ts": datetime.now(),
        "case": case,
        "action": action
    })                                                                                                                                                                                                                                                                                                                                                                                     count_status.py                                                                                     000644  000765  000024  00000002622 14516104451 015205  0                                                                                                    ustar 00silversu                        staff                           000000  000000                                                                                                                                                                         from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import common_variable

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " Start Coupon Summary")
conn = MongoClient(common_variable.MONGODB_URI)

coupons = conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON].find({},{"_id": 1})

for coupon in coupons:
    coupon_id = coupon["_id"]
    status_result = list(conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].aggregate([
        {
            '$match': {
                'coupon_id': coupon_id
            }
        }, {
            '$group': {
                '_id': '$status', 
                'cnt': {
                    '$sum': 1
                }
            }
        }
    ]))
    if len(status_result) == 0:
        conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON].delete_one({"_id":coupon_id})
    else:
        summary = {
            "coupon_id": coupon_id
        }
        total = 0
        for each in status_result:
            status = each["_id"]
            count = each["cnt"]
            total = total + int(count)
            summary[status] = count
        summary["total"] = total
        conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SUMMARY].replace_one(
            { "coupon_id": coupon_id}, summary, upsert=True
        )


print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " Finish Coupon Summary")                                                                                                              gen_coupon.py                                                                                       000644  000765  000024  00000020425 14510321730 014602  0                                                                                                    ustar 00silversu                        staff                           000000  000000                                                                                                                                                                         from pymongo import MongoClient
from threading import Thread
from argparse import ArgumentParser
from datetime import datetime
import random, string, bson
import common_variable

TOTAL = 50000
SLAVE = 10000
TAGS = ["電子","家用","限時優惠","日常","辦公","汽車","機車","旅遊","玩具","母嬰","運動","周邊","精選","通訊","數位","食品","寵物","生鮮","居家","書籍"]
MEMBER_TIER = ["A","B","C","D"]

conn = MongoClient(common_variable.MONGODB_URI)

def generate_coupon(i: float, num: float):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " " +str(i)+ " start")
    
    coupons = []
    for j in range(num):
        coupon_id = bson.ObjectId()
        member_tier = random.sample(MEMBER_TIER, k=random.randint(1, 4))
        coupons.append({
            "_id": coupon_id,
            "compaign_tags": random.sample(TAGS, k=random.randint(2, 7)),
            "member_tier": member_tier,
            "field1": ''.join(random.choices(string.ascii_letters, k=13)),
            "field2": ''.join(random.choices(string.ascii_letters, k=13)),
            "field3": ''.join(random.choices(string.ascii_letters, k=13)),
            "field4": ''.join(random.choices(string.ascii_letters, k=13)),
            "field5": ''.join(random.choices(string.ascii_letters, k=13)),
            "field6": ''.join(random.choices(string.ascii_letters, k=13)),
            "field7": ''.join(random.choices(string.ascii_letters, k=13)),
            "field8": ''.join(random.choices(string.ascii_letters, k=13)),
            "field9": ''.join(random.choices(string.ascii_letters, k=13)),
            "field10": ''.join(random.choices(string.ascii_letters, k=13)),
            "field11": ''.join(random.choices(string.ascii_letters, k=13)),
            "field12": ''.join(random.choices(string.ascii_letters, k=13)),
            "field13": ''.join(random.choices(string.ascii_letters, k=13)),
            "field14": ''.join(random.choices(string.ascii_letters, k=13)),
            "field15": ''.join(random.choices(string.ascii_letters, k=13)),
            "field16": ''.join(random.choices(string.ascii_letters, k=13)),
            "field17": ''.join(random.choices(string.ascii_letters, k=13)),
            "field18": ''.join(random.choices(string.ascii_letters, k=13)),
            "field19": ''.join(random.choices(string.ascii_letters, k=13)),
            "field20": ''.join(random.choices(string.ascii_letters, k=13)),
            "field21": ''.join(random.choices(string.ascii_letters, k=13)),
            "field22": ''.join(random.choices(string.ascii_letters, k=13)),
            "field23": ''.join(random.choices(string.ascii_letters, k=13)),
            "field24": ''.join(random.choices(string.ascii_letters, k=13)),
            "field25": ''.join(random.choices(string.ascii_letters, k=13)),
            "field26": ''.join(random.choices(string.ascii_letters, k=13)),
            "field27": ''.join(random.choices(string.ascii_letters, k=13)),
            "field28": ''.join(random.choices(string.ascii_letters, k=13)),
            "field29": ''.join(random.choices(string.ascii_letters, k=13)),
            "field30": ''.join(random.choices(string.ascii_letters, k=13)),
            "field31": ''.join(random.choices(string.ascii_letters, k=13)),
            "field32": ''.join(random.choices(string.ascii_letters, k=13)),
            "field33": ''.join(random.choices(string.ascii_letters, k=13)),
            "field34": ''.join(random.choices(string.ascii_letters, k=13)),
            "field35": ''.join(random.choices(string.ascii_letters, k=13)),
            "field36": ''.join(random.choices(string.ascii_letters, k=13)),
            "field37": ''.join(random.choices(string.ascii_letters, k=13)),
            "field38": ''.join(random.choices(string.ascii_letters, k=13)),
            "field39": ''.join(random.choices(string.ascii_letters, k=13)),
            "field40": ''.join(random.choices(string.ascii_letters, k=13)),
            "field41": ''.join(random.choices(string.ascii_letters, k=13)),
            "field42": ''.join(random.choices(string.ascii_letters, k=13)),
            "field43": ''.join(random.choices(string.ascii_letters, k=13)),
            "field44": ''.join(random.choices(string.ascii_letters, k=13)),
            "field45": ''.join(random.choices(string.ascii_letters, k=13)),
            "field46": ''.join(random.choices(string.ascii_letters, k=13)),
            "field47": ''.join(random.choices(string.ascii_letters, k=13)),
            "field48": ''.join(random.choices(string.ascii_letters, k=13)),
            "field49": ''.join(random.choices(string.ascii_letters, k=13)),
            "field50": ''.join(random.choices(string.ascii_letters, k=13)),
            "field51": ''.join(random.choices(string.ascii_letters, k=13)),
            "field52": ''.join(random.choices(string.ascii_letters, k=13)),
            "field53": ''.join(random.choices(string.ascii_letters, k=13)),
            "field54": ''.join(random.choices(string.ascii_letters, k=13)),
            "field55": ''.join(random.choices(string.ascii_letters, k=13)),
            "field56": ''.join(random.choices(string.ascii_letters, k=13)),
            "field57": ''.join(random.choices(string.ascii_letters, k=13)),
            "field58": ''.join(random.choices(string.ascii_letters, k=13)),
            "field59": ''.join(random.choices(string.ascii_letters, k=13)),
            "field60": ''.join(random.choices(string.ascii_letters, k=13)),
            "field61": ''.join(random.choices(string.ascii_letters, k=13)),
            "field62": ''.join(random.choices(string.ascii_letters, k=13)),
            "field63": ''.join(random.choices(string.ascii_letters, k=13)),
            "field64": ''.join(random.choices(string.ascii_letters, k=13)),
            "field65": ''.join(random.choices(string.ascii_letters, k=13))
        })
        coupon_slave = []
        for k in range(SLAVE):
            coupon_slave.append({
                "main_id": coupon_id,
                "status": "No Take",
                "is_take": False,
                "is_using": False,
                "criteria": {
                    "member_tier": random.choice(member_tier)
                },
                "field1": ''.join(random.choices(string.ascii_letters, k=13)),
                "field2": ''.join(random.choices(string.ascii_letters, k=13)),
                "field3": ''.join(random.choices(string.ascii_letters, k=13)),
                "field4": ''.join(random.choices(string.ascii_letters, k=13)),
                "field5": ''.join(random.choices(string.ascii_letters, k=13)),
                "field6": ''.join(random.choices(string.ascii_letters, k=13)),
                "field7": ''.join(random.choices(string.ascii_letters, k=13)),
                "field8": ''.join(random.choices(string.ascii_letters, k=13)),
                "field9": ''.join(random.choices(string.ascii_letters, k=13))
            })
        conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_COUPON_SLAVE].insert_many(coupon_slave, ordered=False)
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " " +str(coupon_id)+ " commit")
        # if j % 10000 == 0:
        #     conn[MONGODB_DB][MONGODB_COUPON].insert_many(coupons, ordered=False)
        #     coupons = []
        #     print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " " +str(i)+ " commit")
    if len(coupons) > 1:
        conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON].insert_many(coupons, ordered=False)
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " " +str(i)+ " commit")
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " " +str(i)+ " stop")

def main(threads: int):
    num = TOTAL / threads
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " Generate "+str(num)+ " for each thread")
    thread_list = []
    for i in range(threads):
        t = Thread(target=generate_coupon, args=(int(i), int(num),))
        t.start()
        thread_list.append(t)
    
    for i in thread_list:
        i.join()
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " done")

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-t", "--threads", help="Number of Threads", required=False)
    parser.add_argument("-c", "--coupons", help="Number of Coupon", required=True)
    PARSER_ARGS = parser.parse_args()
    threads = PARSER_ARGS.threads
    TOTAL = int(PARSER_ARGS.coupons)
    main(int(threads))
                                                                                                                                                                                                                                           gen_coupon_slave.py                                                                                 000644  000765  000024  00000021506 14514276744 016017  0                                                                                                    ustar 00silversu                        staff                           000000  000000                                                                                                                                                                         """
1.【商店大量發送券給會員】(大資料量)
情境: 發送一檔運費券，會員數共五百萬，每人發 2 張

=> 讀取會員 _id 清單
=> 建立一張主檔
=> 建立 500 萬 * 2 個子檔
=> 批次大量 Insert
"""
from pymongo import MongoClient
from threading import Thread
from argparse import ArgumentParser
from datetime import datetime, timedelta
from multiprocessing import Pool
import random, string, bson
import common_variable, gen_coupon

total = 10000
coupon_id = bson.ObjectId()
members = []
conn = MongoClient(common_variable.MONGODB_URI)

def generate_coupon(slave_for_each_member: int):
    conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON].insert_one({
        "_id": coupon_id,
        "name": "運費券_"+datetime.now().strftime("%Y%m%d%H%M%S"),
        "type": "dispatch",
        "main_criteria": {
            "disp_count": slave_for_each_member,
            "disp_all": True
        },
        "compaign_tags": random.sample(gen_coupon.TAGS, k=random.randint(2, 7)),
        "field1": "".join(random.choices(string.ascii_letters, k=13)),
        "field2": "".join(random.choices(string.ascii_letters, k=13)),
        "field3": "".join(random.choices(string.ascii_letters, k=13)),
        "field4": "".join(random.choices(string.ascii_letters, k=13)),
        "field5": "".join(random.choices(string.ascii_letters, k=13)),
        "field6": "".join(random.choices(string.ascii_letters, k=13)),
        "field7": "".join(random.choices(string.ascii_letters, k=13)),
        "field8": "".join(random.choices(string.ascii_letters, k=13)),
        "field9": "".join(random.choices(string.ascii_letters, k=13)),
        "field10": "".join(random.choices(string.ascii_letters, k=13)),
        "field11": "".join(random.choices(string.ascii_letters, k=13)),
        "field12": "".join(random.choices(string.ascii_letters, k=13)),
        "field13": "".join(random.choices(string.ascii_letters, k=13)),
        "field14": "".join(random.choices(string.ascii_letters, k=13)),
        "field15": "".join(random.choices(string.ascii_letters, k=13)),
        "field16": "".join(random.choices(string.ascii_letters, k=13)),
        "field17": "".join(random.choices(string.ascii_letters, k=13)),
        "field18": "".join(random.choices(string.ascii_letters, k=13)),
        "field19": "".join(random.choices(string.ascii_letters, k=13)),
        "field20": "".join(random.choices(string.ascii_letters, k=13)),
        "field21": "".join(random.choices(string.ascii_letters, k=13)),
        "field22": "".join(random.choices(string.ascii_letters, k=13)),
        "field23": "".join(random.choices(string.ascii_letters, k=13)),
        "field24": "".join(random.choices(string.ascii_letters, k=13)),
        "field25": "".join(random.choices(string.ascii_letters, k=13)),
        "field26": "".join(random.choices(string.ascii_letters, k=13)),
        "field27": "".join(random.choices(string.ascii_letters, k=13)),
        "field28": "".join(random.choices(string.ascii_letters, k=13)),
        "field29": "".join(random.choices(string.ascii_letters, k=13)),
        "field30": "".join(random.choices(string.ascii_letters, k=13)),
        "field31": "".join(random.choices(string.ascii_letters, k=13)),
        "field32": "".join(random.choices(string.ascii_letters, k=13)),
        "field33": "".join(random.choices(string.ascii_letters, k=13)),
        "field34": "".join(random.choices(string.ascii_letters, k=13)),
        "field35": "".join(random.choices(string.ascii_letters, k=13)),
        "field36": "".join(random.choices(string.ascii_letters, k=13)),
        "field37": "".join(random.choices(string.ascii_letters, k=13)),
        "field38": "".join(random.choices(string.ascii_letters, k=13)),
        "field39": "".join(random.choices(string.ascii_letters, k=13)),
        "field40": "".join(random.choices(string.ascii_letters, k=13)),
        "field41": "".join(random.choices(string.ascii_letters, k=13)),
        "field42": "".join(random.choices(string.ascii_letters, k=13)),
        "field43": "".join(random.choices(string.ascii_letters, k=13)),
        "field44": "".join(random.choices(string.ascii_letters, k=13)),
        "field45": "".join(random.choices(string.ascii_letters, k=13)),
        "field46": "".join(random.choices(string.ascii_letters, k=13)),
        "field47": "".join(random.choices(string.ascii_letters, k=13)),
        "field48": "".join(random.choices(string.ascii_letters, k=13)),
        "field49": "".join(random.choices(string.ascii_letters, k=13)),
        "field50": "".join(random.choices(string.ascii_letters, k=13)),
        "field51": "".join(random.choices(string.ascii_letters, k=13)),
        "field52": "".join(random.choices(string.ascii_letters, k=13)),
        "field53": "".join(random.choices(string.ascii_letters, k=13)),
        "field54": "".join(random.choices(string.ascii_letters, k=13)),
        "field55": "".join(random.choices(string.ascii_letters, k=13)),
        "field56": "".join(random.choices(string.ascii_letters, k=13)),
        "field57": "".join(random.choices(string.ascii_letters, k=13)),
        "field58": "".join(random.choices(string.ascii_letters, k=13)),
        "field59": "".join(random.choices(string.ascii_letters, k=13)),
        "field60": "".join(random.choices(string.ascii_letters, k=13)),
        "field61": "".join(random.choices(string.ascii_letters, k=13)),
        "field62": "".join(random.choices(string.ascii_letters, k=13)),
        "field63": "".join(random.choices(string.ascii_letters, k=13)),
        "field64": "".join(random.choices(string.ascii_letters, k=13)),
        "field65": "".join(random.choices(string.ascii_letters, k=13))
    })

def generate_coupon_slaves(i: float, slave_for_each_member: float, members: []):
    conn = MongoClient(common_variable.MONGODB_URI)
    end_date = datetime.now() + timedelta(days=30)
    coupon_slaves = []
    for j in range(len(members)):
        for _ in range(slave_for_each_member):
            coupon_slaves.append({
                "coupon_id": coupon_id,
                "end_date": end_date,
                "member": {
                    "id": members[j]["_id"],
                    "ts": datetime.now()
                },
                "status": "A",
                "field1": "".join(random.choices(string.ascii_letters, k=13)),
                "field2": "".join(random.choices(string.ascii_letters, k=13)),
                "field3": "".join(random.choices(string.ascii_letters, k=13)),
                "field4": "".join(random.choices(string.ascii_letters, k=13)),
                "field5": "".join(random.choices(string.ascii_letters, k=13)),
                "field6": "".join(random.choices(string.ascii_letters, k=13)),
                "field7": "".join(random.choices(string.ascii_letters, k=13)),
                "field8": "".join(random.choices(string.ascii_letters, k=13)),
                "field9": "".join(random.choices(string.ascii_letters, k=13))
            })
        if j > 0 and j % 10000 == 0:
            conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].insert_many(coupon_slaves, ordered=False)
            coupon_slaves = []
    if len(coupon_slaves) > 1:
        conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].insert_many(coupon_slaves, ordered=False)

def main(threads: int, slave_for_each_member: int, members: []):
    num = len(members) / threads
    thread_list = []
    for i in range(threads):
        t = Thread(target=generate_coupon_slaves, args=(int(i), slave_for_each_member, members[int(i * num): int((i+1) * num)], ))
        t.start()
        thread_list.append(t)
    
    for i in thread_list:
        i.join()

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-p", "--processes", help="Number of Processes", required=True)
    parser.add_argument("-t", "--threads", help="Number of Threads", required=True)
    parser.add_argument("-c", "--coupon_slaves", help="Number of Coupon Slaves for each member", required=True)
    PARSER_ARGS = parser.parse_args()

    processes = int(PARSER_ARGS.processes)
    threads = int(PARSER_ARGS.threads)
    slave_for_each_member = int(PARSER_ARGS.coupon_slaves)
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " generate coupon and slave start with coupon_id = "+str(coupon_id))
    
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " get all member_id")
    members = list(conn[common_variable.MONGODB_DB][common_variable.COLL_MEMBER].find({}, {"_id": 1}))
    num_for_process = len(members) / processes

    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " generate coupon main")
    generate_coupon(slave_for_each_member)
    with Pool() as p:
        for x in range(processes):
            p.apply_async(main, (threads, slave_for_each_member, members[int(x * num_for_process):int((x+1) * num_for_process)], ))
        p.close()
        p.join()
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " generate coupon and slave done ")
                                                                                                                                                                                          gen_member.py                                                                                       000644  000765  000024  00000001613 14510322644 014551  0                                                                                                    ustar 00silversu                        staff                           000000  000000                                                                                                                                                                         from pymongo import MongoClient
from datetime import datetime
from argparse import ArgumentParser
import common_variable, names

conn = MongoClient(common_variable.MONGODB_URI)

def generate_member(num: int):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " start")
    members = []
    for i in range(num):
        members.append({
            "name": names.get_full_name()
        })
        if i % 10000 == 0:
            print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " "+ str(i) +" start")
    conn[common_variable.MONGODB_DB][common_variable.COLL_MEMBER].insert_many(members, ordered=False)
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " done")

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-n", "--num", help="Number of Member", required=True)
    PARSER_ARGS = parser.parse_args()
    num = PARSER_ARGS.num
    generate_member(int(num))
                                                                                                                     open_limit_coupon.py                                                                                000644  000765  000024  00000023744 14514345615 016213  0                                                                                                    ustar 00silversu                        staff                           000000  000000                                                                                                                                                                         """
3.【券限領限用】
情境:
3-1. 設定一檔 9 折折價券，會員可於優惠券列表領取，每人至多可領 3 張

=> 前置：建立一張主檔，設定 open_per_limit = 3
=> 啟動 2000 個會員進行領券
=> 領券邏輯：取得 open_per_limit 條件，確認該會員該折扣券已有張數，若有可領取則建立一張子檔並歸戶到會員
"""
from datetime import datetime, timedelta
from locust import User, task, constant, events
from locust.runners import MasterRunner, WorkerRunner
from pymongo import MongoClient
from time import perf_counter_ns
import random, string, bson, logging
import common_variable, gen_coupon

class Mongo:
    conn = ""

    def __init__(self):
        self.conn = MongoClient(common_variable.MONGODB_URI)

    def get_conn(self):
        return self.conn

mongo_client = Mongo()

def load_coupon_id(environment, msg, **kwargs):
    OpenUser.coupon_id = bson.ObjectId(msg.data)

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if isinstance(environment.runner, MasterRunner):
        coupon_id = bson.ObjectId()
        conn = MongoClient(common_variable.MONGODB_URI)
        ## 更新所有曾經啟動過的會員，從 active 變成 inactive
        conn[common_variable.MONGODB_DB][common_variable.COLL_MEMBER].update_many(
            { "active": True }, { "$set": { "active": False } }
        )
        conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON].insert_one({
            "_id": coupon_id,
            "name": "9 折折價券_"+datetime.now().strftime("%Y%m%d%H%M%S"),
            "type": "open",
            "main_criteria": {
                "open_per_limit": 3
            },
            "compaign_tags": random.sample(gen_coupon.TAGS, k=random.randint(2, 7)),
            "field1": "".join(random.choices(string.ascii_letters, k=13)),
            "field2": "".join(random.choices(string.ascii_letters, k=13)),
            "field3": "".join(random.choices(string.ascii_letters, k=13)),
            "field4": "".join(random.choices(string.ascii_letters, k=13)),
            "field5": "".join(random.choices(string.ascii_letters, k=13)),
            "field6": "".join(random.choices(string.ascii_letters, k=13)),
            "field7": "".join(random.choices(string.ascii_letters, k=13)),
            "field8": "".join(random.choices(string.ascii_letters, k=13)),
            "field9": "".join(random.choices(string.ascii_letters, k=13)),
            "field10": "".join(random.choices(string.ascii_letters, k=13)),
            "field11": "".join(random.choices(string.ascii_letters, k=13)),
            "field12": "".join(random.choices(string.ascii_letters, k=13)),
            "field13": "".join(random.choices(string.ascii_letters, k=13)),
            "field14": "".join(random.choices(string.ascii_letters, k=13)),
            "field15": "".join(random.choices(string.ascii_letters, k=13)),
            "field16": "".join(random.choices(string.ascii_letters, k=13)),
            "field17": "".join(random.choices(string.ascii_letters, k=13)),
            "field18": "".join(random.choices(string.ascii_letters, k=13)),
            "field19": "".join(random.choices(string.ascii_letters, k=13)),
            "field20": "".join(random.choices(string.ascii_letters, k=13)),
            "field21": "".join(random.choices(string.ascii_letters, k=13)),
            "field22": "".join(random.choices(string.ascii_letters, k=13)),
            "field23": "".join(random.choices(string.ascii_letters, k=13)),
            "field24": "".join(random.choices(string.ascii_letters, k=13)),
            "field25": "".join(random.choices(string.ascii_letters, k=13)),
            "field26": "".join(random.choices(string.ascii_letters, k=13)),
            "field27": "".join(random.choices(string.ascii_letters, k=13)),
            "field28": "".join(random.choices(string.ascii_letters, k=13)),
            "field29": "".join(random.choices(string.ascii_letters, k=13)),
            "field30": "".join(random.choices(string.ascii_letters, k=13)),
            "field31": "".join(random.choices(string.ascii_letters, k=13)),
            "field32": "".join(random.choices(string.ascii_letters, k=13)),
            "field33": "".join(random.choices(string.ascii_letters, k=13)),
            "field34": "".join(random.choices(string.ascii_letters, k=13)),
            "field35": "".join(random.choices(string.ascii_letters, k=13)),
            "field36": "".join(random.choices(string.ascii_letters, k=13)),
            "field37": "".join(random.choices(string.ascii_letters, k=13)),
            "field38": "".join(random.choices(string.ascii_letters, k=13)),
            "field39": "".join(random.choices(string.ascii_letters, k=13)),
            "field40": "".join(random.choices(string.ascii_letters, k=13)),
            "field41": "".join(random.choices(string.ascii_letters, k=13)),
            "field42": "".join(random.choices(string.ascii_letters, k=13)),
            "field43": "".join(random.choices(string.ascii_letters, k=13)),
            "field44": "".join(random.choices(string.ascii_letters, k=13)),
            "field45": "".join(random.choices(string.ascii_letters, k=13)),
            "field46": "".join(random.choices(string.ascii_letters, k=13)),
            "field47": "".join(random.choices(string.ascii_letters, k=13)),
            "field48": "".join(random.choices(string.ascii_letters, k=13)),
            "field49": "".join(random.choices(string.ascii_letters, k=13)),
            "field50": "".join(random.choices(string.ascii_letters, k=13)),
            "field51": "".join(random.choices(string.ascii_letters, k=13)),
            "field52": "".join(random.choices(string.ascii_letters, k=13)),
            "field53": "".join(random.choices(string.ascii_letters, k=13)),
            "field54": "".join(random.choices(string.ascii_letters, k=13)),
            "field55": "".join(random.choices(string.ascii_letters, k=13)),
            "field56": "".join(random.choices(string.ascii_letters, k=13)),
            "field57": "".join(random.choices(string.ascii_letters, k=13)),
            "field58": "".join(random.choices(string.ascii_letters, k=13)),
            "field59": "".join(random.choices(string.ascii_letters, k=13)),
            "field60": "".join(random.choices(string.ascii_letters, k=13)),
            "field61": "".join(random.choices(string.ascii_letters, k=13)),
            "field62": "".join(random.choices(string.ascii_letters, k=13)),
            "field63": "".join(random.choices(string.ascii_letters, k=13)),
            "field64": "".join(random.choices(string.ascii_letters, k=13)),
            "field65": "".join(random.choices(string.ascii_letters, k=13))
        })
        logging.info("Start Test Case 3-1 with coupon_id: "+str(coupon_id))
        environment.runner.send_message('coupon_id', coupon_id.binary)
    elif isinstance(environment.runner, WorkerRunner):
        environment.runner.register_message('coupon_id', load_coupon_id)
    else:
        logging.info("!!!")

class OpenUser(User):
    wait_time = constant(0)
    coupon_id = None

    def get_coupon(self, coupon_id, member_id):
        conn = mongo_client.get_conn()
        ## 取得 Coupon 領取數量條件
        open_per_limit = conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON].find_one(
            { "_id": coupon_id }, { "main_criteria.open_per_limit": 1 }
        )["main_criteria"]["open_per_limit"]
        ## 取得當前會員已領取數量
        member_get_count = conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].count_documents(
            { "coupon_id": coupon_id, "member.id": member_id}
        )

        ## 檢查領取條件
        if member_get_count < open_per_limit:
            conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].insert_one({
                "coupon_id": coupon_id,
                "end_date": datetime.now() + timedelta(days=30),
                "member": {
                    "id": member_id,
                    "ts": datetime.now()
                },
                "status": "A",
                "field1": "".join(random.choices(string.ascii_letters, k=13)),
                "field2": "".join(random.choices(string.ascii_letters, k=13)),
                "field3": "".join(random.choices(string.ascii_letters, k=13)),
                "field4": "".join(random.choices(string.ascii_letters, k=13)),
                "field5": "".join(random.choices(string.ascii_letters, k=13)),
                "field6": "".join(random.choices(string.ascii_letters, k=13)),
                "field7": "".join(random.choices(string.ascii_letters, k=13)),
                "field8": "".join(random.choices(string.ascii_letters, k=13)),
                "field9": "".join(random.choices(string.ascii_letters, k=13))
            })
            return True
        else:
            return False

    @task
    def run_get_coupon(self):
        member_id = None
        ## 隨機取得一個會員
        result = mongo_client.get_conn()[common_variable.MONGODB_DB][common_variable.COLL_MEMBER].find_one_and_update(
            { "active": False }, { "$set": { "active": True } }
        )
        if result:
            member_id = result["_id"]
        else:
            self.environment.runner.quit()
        
        for _ in range(4):
            start_time = perf_counter_ns()
            result = self.get_coupon(self.coupon_id, member_id)
            duration = (perf_counter_ns() - start_time) / 1000000
            if result:
                self.environment.events.request.fire(
                    request_type="GET", 
                    name="Get Coupon",
                    response_time=duration, 
                    response_length=0,
                    context=member_id,
                    exception=None
                )
            else:
                self.environment.events.request.fire(
                    request_type="GET", 
                    name="Get Coupon",
                    response_time=duration, 
                    response_length=0,
                    context=member_id,
                    exception=str(member_id)+": 無可領促銷券"
                )                            test.py                                                                                             000644  000765  000024  00000002753 14513652467 013452  0                                                                                                    ustar 00silversu                        staff                           000000  000000                                                                                                                                                                         from locust import User, task, constant, events
from locust.runners import MasterRunner, WorkerRunner
import bson, logging

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if isinstance(environment.runner, WorkerRunner):
        environment.runner.register_message('count', load_cnt)
        logging.info("Register at worker")

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    if isinstance(environment.runner, MasterRunner):
        tmp = bson.ObjectId()
        logging.info("master: "+str(tmp))
        environment.runner.send_message('count', tmp.binary)
        logging.info("Send from master")

def load_cnt(environment, msg, **kwargs):
    logging.info("Get from master: "+str(msg.data))
    TestUser.cnt = bson.ObjectId(msg.data)

class TestUser(User):
    wait_time = constant(0)
    cnt = None
    @task
    def print_count(self):
        logging.info(self.cnt)
        if self.cnt:
            self.environment.events.request.fire(
                request_type="GET", 
                name="Test Count",
                response_time=0, 
                response_length=0,
                context=self.cnt,
                exception=None
            )
        else:
            self.environment.events.request.fire(
                request_type="GET", 
                name="Test Count",
                response_time=0, 
                response_length=0,
                context=self.cnt,
                exception="Cnt is still zero"
            )                     test_tx.py                                                                                          000644  000765  000024  00000004701 14510336207 014144  0                                                                                                    ustar 00silversu                        staff                           000000  000000                                                                                                                                                                         from pymongo import MongoClient, errors, read_concern, write_concern, read_preferences
from threading import Thread
from argparse import ArgumentParser
from datetime import datetime
import random, string, bson
import common_variable, names

conn = MongoClient(common_variable.MONGODB_URI)

def run_transaction_with_retry(txn_func, session):
    while True:
        try:
            txn_func(session)  # performs transaction
            break
        except (errors.ConnectionFailure, errors.OperationFailure) as exc:
            # If transient error, retry the whole transaction
            if exc.has_error_label("TransientTransactionError"):
                print("TransientTransactionError, retrying transaction ...")
                continue
            else:
                raise
def commit_with_retry(session):
    while True:
        try:
            # Commit uses write concern set at transaction start.
            session.commit_transaction()
            print("Transaction committed.")
            break
        except (errors.ConnectionFailure, errors.OperationFailure) as exc:
            # Can retry commit
            if exc.has_error_label("UnknownTransactionCommitResult"):
                print("UnknownTransactionCommitResult, retrying commit operation ...")
                continue
            else:
                print("Error during commit ...")
                raise
def insert_tx(session):
    with session.start_transaction(
        read_concern=read_concern.ReadConcern("snapshot"),
        write_concern=write_concern.WriteConcern(w="majority"),
        read_preference=read_preferences.ReadPreference.PRIMARY,
    ):
        if session.client[common_variable.MONGODB_DB]["tx"].find_one_and_update(
            {"_id":"tx", "coupon":{"$gt": 0}}, 
            {"$inc":{"coupon": -1}}, session=session) == None:
            raise
        session.client[common_variable.MONGODB_DB]["tx"].insert_one({'i':2,'name': names.get_full_name()}, session=session)
        session.client[common_variable.MONGODB_DB]["tx"].insert_one({'i':3,'name': names.get_full_name()}, session=session)

if __name__ == "__main__":
    with conn.start_session() as session:
        for i in range(100):
            try:
                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " tx start")
                run_transaction_with_retry(insert_tx, session)
                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ " tx done")
            except Exception:
                raise                                                               tx_multi_coupon.py                                                                                  000644  000765  000024  00000025361 14514027671 015715  0                                                                                                    ustar 00silversu                        staff                           000000  000000                                                                                                                                                                         """
3.【券限領限用】
情境：
3-5. 會員於購物車一次使用 3 張折價券，這 3 張折價券要一次核銷 (transaction)

=> 前置：建立一張主檔
=> 前置：建立 500 * 3 張子檔
=> 啟動 2000 個會員進行用券
=> 用券邏輯：開啟 transaction，一次核銷三張子檔，成功 commit，異常 rollback
"""

from datetime import datetime, timedelta
from locust import User, task, constant, events
from locust.runners import MasterRunner, WorkerRunner
from pymongo import MongoClient, read_concern, write_concern, read_preferences
from time import perf_counter_ns
import random, string, bson, logging
import common_variable, gen_coupon

class Mongo:
    conn = ""

    def __init__(self):
        self.conn = MongoClient(common_variable.MONGODB_URI)

    def get_conn(self):
        return self.conn

mongo_client = Mongo()

def load_coupon_id(environment, msg, **kwargs):
    TxUser.coupon_id = bson.ObjectId(msg.data)

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if isinstance(environment.runner, MasterRunner):
        coupon_id = bson.ObjectId()
        members = []
        conn = MongoClient(common_variable.MONGODB_URI)
        ## 更新所有曾經啟動過的會員，從 active 變成 inactive
        conn[common_variable.MONGODB_DB][common_variable.COLL_MEMBER].update_many(
            { "active": True }, { "$set": { "active": False } }
        )
        conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON].insert_one({
            "_id": coupon_id,
            "name": "X 折折價券_"+datetime.now().strftime("%Y%m%d%H%M%S"),
            "type": "dispatch",
            "compaign_tags": random.sample(gen_coupon.TAGS, k=random.randint(2, 7)),
            "field1": "".join(random.choices(string.ascii_letters, k=13)),
            "field2": "".join(random.choices(string.ascii_letters, k=13)),
            "field3": "".join(random.choices(string.ascii_letters, k=13)),
            "field4": "".join(random.choices(string.ascii_letters, k=13)),
            "field5": "".join(random.choices(string.ascii_letters, k=13)),
            "field6": "".join(random.choices(string.ascii_letters, k=13)),
            "field7": "".join(random.choices(string.ascii_letters, k=13)),
            "field8": "".join(random.choices(string.ascii_letters, k=13)),
            "field9": "".join(random.choices(string.ascii_letters, k=13)),
            "field10": "".join(random.choices(string.ascii_letters, k=13)),
            "field11": "".join(random.choices(string.ascii_letters, k=13)),
            "field12": "".join(random.choices(string.ascii_letters, k=13)),
            "field13": "".join(random.choices(string.ascii_letters, k=13)),
            "field14": "".join(random.choices(string.ascii_letters, k=13)),
            "field15": "".join(random.choices(string.ascii_letters, k=13)),
            "field16": "".join(random.choices(string.ascii_letters, k=13)),
            "field17": "".join(random.choices(string.ascii_letters, k=13)),
            "field18": "".join(random.choices(string.ascii_letters, k=13)),
            "field19": "".join(random.choices(string.ascii_letters, k=13)),
            "field20": "".join(random.choices(string.ascii_letters, k=13)),
            "field21": "".join(random.choices(string.ascii_letters, k=13)),
            "field22": "".join(random.choices(string.ascii_letters, k=13)),
            "field23": "".join(random.choices(string.ascii_letters, k=13)),
            "field24": "".join(random.choices(string.ascii_letters, k=13)),
            "field25": "".join(random.choices(string.ascii_letters, k=13)),
            "field26": "".join(random.choices(string.ascii_letters, k=13)),
            "field27": "".join(random.choices(string.ascii_letters, k=13)),
            "field28": "".join(random.choices(string.ascii_letters, k=13)),
            "field29": "".join(random.choices(string.ascii_letters, k=13)),
            "field30": "".join(random.choices(string.ascii_letters, k=13)),
            "field31": "".join(random.choices(string.ascii_letters, k=13)),
            "field32": "".join(random.choices(string.ascii_letters, k=13)),
            "field33": "".join(random.choices(string.ascii_letters, k=13)),
            "field34": "".join(random.choices(string.ascii_letters, k=13)),
            "field35": "".join(random.choices(string.ascii_letters, k=13)),
            "field36": "".join(random.choices(string.ascii_letters, k=13)),
            "field37": "".join(random.choices(string.ascii_letters, k=13)),
            "field38": "".join(random.choices(string.ascii_letters, k=13)),
            "field39": "".join(random.choices(string.ascii_letters, k=13)),
            "field40": "".join(random.choices(string.ascii_letters, k=13)),
            "field41": "".join(random.choices(string.ascii_letters, k=13)),
            "field42": "".join(random.choices(string.ascii_letters, k=13)),
            "field43": "".join(random.choices(string.ascii_letters, k=13)),
            "field44": "".join(random.choices(string.ascii_letters, k=13)),
            "field45": "".join(random.choices(string.ascii_letters, k=13)),
            "field46": "".join(random.choices(string.ascii_letters, k=13)),
            "field47": "".join(random.choices(string.ascii_letters, k=13)),
            "field48": "".join(random.choices(string.ascii_letters, k=13)),
            "field49": "".join(random.choices(string.ascii_letters, k=13)),
            "field50": "".join(random.choices(string.ascii_letters, k=13)),
            "field51": "".join(random.choices(string.ascii_letters, k=13)),
            "field52": "".join(random.choices(string.ascii_letters, k=13)),
            "field53": "".join(random.choices(string.ascii_letters, k=13)),
            "field54": "".join(random.choices(string.ascii_letters, k=13)),
            "field55": "".join(random.choices(string.ascii_letters, k=13)),
            "field56": "".join(random.choices(string.ascii_letters, k=13)),
            "field57": "".join(random.choices(string.ascii_letters, k=13)),
            "field58": "".join(random.choices(string.ascii_letters, k=13)),
            "field59": "".join(random.choices(string.ascii_letters, k=13)),
            "field60": "".join(random.choices(string.ascii_letters, k=13)),
            "field61": "".join(random.choices(string.ascii_letters, k=13)),
            "field62": "".join(random.choices(string.ascii_letters, k=13)),
            "field63": "".join(random.choices(string.ascii_letters, k=13)),
            "field64": "".join(random.choices(string.ascii_letters, k=13)),
            "field65": "".join(random.choices(string.ascii_letters, k=13))
        })

        members = list(conn[common_variable.MONGODB_DB][common_variable.COLL_MEMBER].find({}, {"_id": 1}).limit(10000))
        coupon_slaves = []
        locust_members = []
        end_date = datetime.now() + timedelta(days=2)
        for member in members:
            for _ in range(3):
                coupon_slaves.append({
                    "coupon_id": coupon_id,
                    "end_date": end_date,
                    "member": {
                        "id": member["_id"],
                        "ts": datetime.now()
                    },
                    "status": "A",
                    "field1": "".join(random.choices(string.ascii_letters, k=13)),
                    "field2": "".join(random.choices(string.ascii_letters, k=13)),
                    "field3": "".join(random.choices(string.ascii_letters, k=13)),
                    "field4": "".join(random.choices(string.ascii_letters, k=13)),
                    "field5": "".join(random.choices(string.ascii_letters, k=13)),
                    "field6": "".join(random.choices(string.ascii_letters, k=13)),
                    "field7": "".join(random.choices(string.ascii_letters, k=13)),
                    "field8": "".join(random.choices(string.ascii_letters, k=13)),
                    "field9": "".join(random.choices(string.ascii_letters, k=13))
                })
            locust_members.append({ "member_id": member["_id"], "coupon_id": coupon_id })
        conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].insert_many(coupon_slaves, ordered=False)
        conn[common_variable.MONGODB_DB][common_variable.COLL_LOCUST_MEMBER].insert_many(locust_members, ordered=False)
        logging.info("Generate "+str(len(locust_members))+ " members for testing")
        logging.info("Start Test Case 3-5 with coupon_id: "+str(coupon_id))
        environment.runner.send_message('coupon_id', coupon_id.binary)
    elif isinstance(environment.runner, WorkerRunner):
        environment.runner.register_message('coupon_id', load_coupon_id)
    else:
        logging.info("!!!")

class TxUser(User):
    wait_time = constant(0)
    coupon_id = None
    
    def exec_multi_coupon_tx(self, session, coupon_slaves: []):
        coll_coupon_slave = session.client[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE]
        for coupon_slave in coupon_slaves:
            coll_coupon_slave.update_one(
                { "_id": coupon_slave["_id"] }, { "$set": { "status": "D" }}
            , session=session)

    def multi_coupon_tx(self, coupon_slaves: []):
        conn = mongo_client.get_conn()
        with conn.start_session() as session:
            session.with_transaction(
                lambda s: self.exec_multi_coupon_tx(s, coupon_slaves),
                read_concern=read_concern.ReadConcern("snapshot"),
                write_concern=write_concern.WriteConcern(w="majority"),
                read_preference=read_preferences.ReadPreference.PRIMARY,
            )

    @task
    def run_multi_coupon_tx(self):
        conn = mongo_client.get_conn()
        
        # 取得測試會員
        member = conn[common_variable.MONGODB_DB][common_variable.COLL_LOCUST_MEMBER].find_one_and_delete({ "coupon_id": self.coupon_id })
        # 判斷是否還有可測試的會員
        if not member:
            self.environment.events.request.fire(
                request_type="GET", 
                name="Tx Coupon",
                response_time=0, 
                response_length=0,
                context="",
                exception="無可測試的會員"
            )
            return
        
        member_id = member["member_id"]
        coupon_slaves = list(conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].find(
            { "coupon_id": self.coupon_id, "member.id": member_id }
        ))
        # logging.info("Run memeber = "+str(member_id))
        
        start_time = perf_counter_ns()
        self.multi_coupon_tx(coupon_slaves)
        duration = (perf_counter_ns() - start_time) / 1000000
        self.environment.events.request.fire(
            request_type="GET", 
            name="Tx Coupon",
            response_time=duration, 
            response_length=0,
            context=member_id,
            exception=None
        )
                                                                                                                                                                                                                                                                               use_limit_coupon.py                                                                                 000644  000765  000024  00000026202 14514012560 016024  0                                                                                                    ustar 00silversu                        staff                           000000  000000                                                                                                                                                                         """
3.【券限領限用】
情境:
3-2. 廣發 5 折折價券，僅前 100 個會員可以核銷

=> 前置：建立一張主檔，設定 use_limit = 100
=> 前置：建立 500 萬張子檔給每個會員
=> 啟動 2000 個會員進行用券
=> 用券邏輯：判斷 use_limit > 0，扣除一個 use_limit，核銷該會員子券
"""
from datetime import datetime, timedelta
from locust import User, task, constant, events
from locust.runners import MasterRunner, WorkerRunner
from pymongo import MongoClient
from time import perf_counter_ns
import random, string, bson, logging
import common_variable, gen_coupon

class Mongo:
    conn = ""

    def __init__(self):
        self.conn = MongoClient(common_variable.MONGODB_URI)

    def get_conn(self):
        return self.conn

mongo_client = Mongo()

def load_coupon_id(environment, msg, **kwargs):
    UseLimitUser.coupon_id = bson.ObjectId(msg.data)

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if isinstance(environment.runner, MasterRunner):
        coupon_id = bson.ObjectId()
        conn = MongoClient(common_variable.MONGODB_URI)
        ## 更新所有曾經啟動過的會員，從 active 變成 inactive
        conn[common_variable.MONGODB_DB][common_variable.COLL_MEMBER].update_many(
            { "active": True }, { "$set": { "active": False } }
        )
        conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON].insert_one({
            "_id": coupon_id,
            "name": "5 折折價券_"+datetime.now().strftime("%Y%m%d%H%M%S"),
            "type": "open",
            "main_criteria": {
                "use_limit": 100
            },
            "compaign_tags": random.sample(gen_coupon.TAGS, k=random.randint(2, 7)),
            "field1": "".join(random.choices(string.ascii_letters, k=13)),
            "field2": "".join(random.choices(string.ascii_letters, k=13)),
            "field3": "".join(random.choices(string.ascii_letters, k=13)),
            "field4": "".join(random.choices(string.ascii_letters, k=13)),
            "field5": "".join(random.choices(string.ascii_letters, k=13)),
            "field6": "".join(random.choices(string.ascii_letters, k=13)),
            "field7": "".join(random.choices(string.ascii_letters, k=13)),
            "field8": "".join(random.choices(string.ascii_letters, k=13)),
            "field9": "".join(random.choices(string.ascii_letters, k=13)),
            "field10": "".join(random.choices(string.ascii_letters, k=13)),
            "field11": "".join(random.choices(string.ascii_letters, k=13)),
            "field12": "".join(random.choices(string.ascii_letters, k=13)),
            "field13": "".join(random.choices(string.ascii_letters, k=13)),
            "field14": "".join(random.choices(string.ascii_letters, k=13)),
            "field15": "".join(random.choices(string.ascii_letters, k=13)),
            "field16": "".join(random.choices(string.ascii_letters, k=13)),
            "field17": "".join(random.choices(string.ascii_letters, k=13)),
            "field18": "".join(random.choices(string.ascii_letters, k=13)),
            "field19": "".join(random.choices(string.ascii_letters, k=13)),
            "field20": "".join(random.choices(string.ascii_letters, k=13)),
            "field21": "".join(random.choices(string.ascii_letters, k=13)),
            "field22": "".join(random.choices(string.ascii_letters, k=13)),
            "field23": "".join(random.choices(string.ascii_letters, k=13)),
            "field24": "".join(random.choices(string.ascii_letters, k=13)),
            "field25": "".join(random.choices(string.ascii_letters, k=13)),
            "field26": "".join(random.choices(string.ascii_letters, k=13)),
            "field27": "".join(random.choices(string.ascii_letters, k=13)),
            "field28": "".join(random.choices(string.ascii_letters, k=13)),
            "field29": "".join(random.choices(string.ascii_letters, k=13)),
            "field30": "".join(random.choices(string.ascii_letters, k=13)),
            "field31": "".join(random.choices(string.ascii_letters, k=13)),
            "field32": "".join(random.choices(string.ascii_letters, k=13)),
            "field33": "".join(random.choices(string.ascii_letters, k=13)),
            "field34": "".join(random.choices(string.ascii_letters, k=13)),
            "field35": "".join(random.choices(string.ascii_letters, k=13)),
            "field36": "".join(random.choices(string.ascii_letters, k=13)),
            "field37": "".join(random.choices(string.ascii_letters, k=13)),
            "field38": "".join(random.choices(string.ascii_letters, k=13)),
            "field39": "".join(random.choices(string.ascii_letters, k=13)),
            "field40": "".join(random.choices(string.ascii_letters, k=13)),
            "field41": "".join(random.choices(string.ascii_letters, k=13)),
            "field42": "".join(random.choices(string.ascii_letters, k=13)),
            "field43": "".join(random.choices(string.ascii_letters, k=13)),
            "field44": "".join(random.choices(string.ascii_letters, k=13)),
            "field45": "".join(random.choices(string.ascii_letters, k=13)),
            "field46": "".join(random.choices(string.ascii_letters, k=13)),
            "field47": "".join(random.choices(string.ascii_letters, k=13)),
            "field48": "".join(random.choices(string.ascii_letters, k=13)),
            "field49": "".join(random.choices(string.ascii_letters, k=13)),
            "field50": "".join(random.choices(string.ascii_letters, k=13)),
            "field51": "".join(random.choices(string.ascii_letters, k=13)),
            "field52": "".join(random.choices(string.ascii_letters, k=13)),
            "field53": "".join(random.choices(string.ascii_letters, k=13)),
            "field54": "".join(random.choices(string.ascii_letters, k=13)),
            "field55": "".join(random.choices(string.ascii_letters, k=13)),
            "field56": "".join(random.choices(string.ascii_letters, k=13)),
            "field57": "".join(random.choices(string.ascii_letters, k=13)),
            "field58": "".join(random.choices(string.ascii_letters, k=13)),
            "field59": "".join(random.choices(string.ascii_letters, k=13)),
            "field60": "".join(random.choices(string.ascii_letters, k=13)),
            "field61": "".join(random.choices(string.ascii_letters, k=13)),
            "field62": "".join(random.choices(string.ascii_letters, k=13)),
            "field63": "".join(random.choices(string.ascii_letters, k=13)),
            "field64": "".join(random.choices(string.ascii_letters, k=13)),
            "field65": "".join(random.choices(string.ascii_letters, k=13))
        })
        members = list(conn[common_variable.MONGODB_DB][common_variable.COLL_MEMBER].find({}, {"_id": 1}).limit(10000))
        coupon_slaves = []
        end_date = datetime.now() + timedelta(days=10)
        for member in members:
            coupon_slaves.append({
                "coupon_id": coupon_id,
                "end_date": end_date,
                "member": {
                    "id": member["_id"],
                    "ts": datetime.now()
                },
                "status": "A",
                "field1": "".join(random.choices(string.ascii_letters, k=13)),
                "field2": "".join(random.choices(string.ascii_letters, k=13)),
                "field3": "".join(random.choices(string.ascii_letters, k=13)),
                "field4": "".join(random.choices(string.ascii_letters, k=13)),
                "field5": "".join(random.choices(string.ascii_letters, k=13)),
                "field6": "".join(random.choices(string.ascii_letters, k=13)),
                "field7": "".join(random.choices(string.ascii_letters, k=13)),
                "field8": "".join(random.choices(string.ascii_letters, k=13)),
                "field9": "".join(random.choices(string.ascii_letters, k=13))
            })
        conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].insert_many(coupon_slaves, ordered=False)
        logging.info("Start Test Case 3-2 with coupon_id: "+str(coupon_id))
        environment.runner.send_message('coupon_id', coupon_id.binary)
    elif isinstance(environment.runner, WorkerRunner):
        environment.runner.register_message('coupon_id', load_coupon_id)
    else:
        logging.info("!!!")

class UseLimitUser(User):
    wait_time = constant(0)
    coupon_id = None

    def use_limit_coupon(self, coupon_slave_id, member_id):
        conn = mongo_client.get_conn()
        ## 檢查是否有該卷可用
        available_coupon_slave = conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].find_one(
            { "_id": coupon_slave_id, "member.id": member_id, "status": { "$ne": "D"} }
        )
        if available_coupon_slave:
            ## 搶使用
            result = conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON].find_one_and_update(
                {
                    "_id": self.coupon_id,
                    "main_criteria.use_limit": {
                        "$gt": 0
                    }
                }, {
                    "$inc": {
                        "main_criteria.use_limit": -1
                    }
                }
            )
            if result:
                # 扣除使用
                conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].update_one(
                    { "_id": coupon_slave_id }, { "$set":{ "status": "D" } }
                )
                logging.info(coupon_slave_id)
                return "T"
            else:
                return "F"
        else:
            return None

    @task
    def run_use_limit_coupon(self):
        member_id = None
        ## 查詢有此促銷券的會員
        result = mongo_client.get_conn()[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].find_one_and_update(
            { "coupon_id": self.coupon_id, "status": "A" }, { "$set": { "status": "P" } }
        )
        if result:
            member_id = result["member"]["id"]
            coupon_slave_id = result["_id"]
        else:
            self.environment.runner.quit()
        
        start_time = perf_counter_ns()
        result = self.use_limit_coupon(coupon_slave_id, member_id)
        duration = (perf_counter_ns() - start_time) / 1000000
        if result:
            if result == "T":
                self.environment.events.request.fire(
                    request_type="GET", 
                    name="Use Limit Coupon Slave",
                    response_time=duration, 
                    response_length=0,
                    context=member_id,
                    exception=None
                )
            else:
                self.environment.events.request.fire(
                    request_type="GET", 
                    name="Use Limit Coupon Slave",
                    response_time=duration, 
                    response_length=0,
                    context=member_id,
                    exception="已達到總用量上限"
                )
        else:
            self.environment.events.request.fire(
                request_type="GET", 
                name="Use Coupon",
                response_time=duration, 
                response_length=0,
                context=member_id,
                exception=str(member_id)+": 無可用促銷券"
            )                                                                                                                                                                                                                                                                                                                                                                                              use_limit_coupon_slave.py                                                                           000644  000765  000024  00000025676 14515620016 017236  0                                                                                                    ustar 00silversu                        staff                           000000  000000                                                                                                                                                                         """
3.【券限領限用】
情境:
3-4. 設定一檔 8 折折價券，該檔券一張可以用 3 次，會員有 3 張，故可以核銷 3 * 3 = 9 次

=> 前置：建立一張主檔，設定 子檔核銷數(slave_criteria.use_limit) = 3
=> 前置：建立 500 萬 * 3 個子檔
=> 啟動 2000 個會員進行用券
=> 用券邏輯：判斷該主檔類型的子檔是否有 use_limit > 0, 扣除該子檔的 use_limit
"""
from datetime import datetime, timedelta
from locust import User, task, constant, events
from locust.runners import MasterRunner, WorkerRunner
from pymongo import MongoClient, ReturnDocument
from time import perf_counter_ns
import random, string, bson, logging
import common_variable, gen_coupon

class Mongo:
    conn = ""

    def __init__(self):
        self.conn = MongoClient(common_variable.MONGODB_URI)

    def get_conn(self):
        return self.conn

mongo_client = Mongo()

def load_coupon_id(environment, msg, **kwargs):
    UseLimitUser.coupon_id = bson.ObjectId(msg.data)

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if isinstance(environment.runner, MasterRunner):
        coupon_id = bson.ObjectId()
        conn = MongoClient(common_variable.MONGODB_URI)
        ## 更新所有曾經啟動過的會員，從 active 變成 inactive
        conn[common_variable.MONGODB_DB][common_variable.COLL_MEMBER].update_many(
            { "active": True }, { "$set": { "active": False } }
        )
        conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON].insert_one({
            "_id": coupon_id,
            "name": "8 折折價券_"+datetime.now().strftime("%Y%m%d%H%M%S"),
            "type": "open",
            "slave_criteria": {
                "use_limit": 3
            },
            "compaign_tags": random.sample(gen_coupon.TAGS, k=random.randint(2, 7)),
            "field1": "".join(random.choices(string.ascii_letters, k=13)),
            "field2": "".join(random.choices(string.ascii_letters, k=13)),
            "field3": "".join(random.choices(string.ascii_letters, k=13)),
            "field4": "".join(random.choices(string.ascii_letters, k=13)),
            "field5": "".join(random.choices(string.ascii_letters, k=13)),
            "field6": "".join(random.choices(string.ascii_letters, k=13)),
            "field7": "".join(random.choices(string.ascii_letters, k=13)),
            "field8": "".join(random.choices(string.ascii_letters, k=13)),
            "field9": "".join(random.choices(string.ascii_letters, k=13)),
            "field10": "".join(random.choices(string.ascii_letters, k=13)),
            "field11": "".join(random.choices(string.ascii_letters, k=13)),
            "field12": "".join(random.choices(string.ascii_letters, k=13)),
            "field13": "".join(random.choices(string.ascii_letters, k=13)),
            "field14": "".join(random.choices(string.ascii_letters, k=13)),
            "field15": "".join(random.choices(string.ascii_letters, k=13)),
            "field16": "".join(random.choices(string.ascii_letters, k=13)),
            "field17": "".join(random.choices(string.ascii_letters, k=13)),
            "field18": "".join(random.choices(string.ascii_letters, k=13)),
            "field19": "".join(random.choices(string.ascii_letters, k=13)),
            "field20": "".join(random.choices(string.ascii_letters, k=13)),
            "field21": "".join(random.choices(string.ascii_letters, k=13)),
            "field22": "".join(random.choices(string.ascii_letters, k=13)),
            "field23": "".join(random.choices(string.ascii_letters, k=13)),
            "field24": "".join(random.choices(string.ascii_letters, k=13)),
            "field25": "".join(random.choices(string.ascii_letters, k=13)),
            "field26": "".join(random.choices(string.ascii_letters, k=13)),
            "field27": "".join(random.choices(string.ascii_letters, k=13)),
            "field28": "".join(random.choices(string.ascii_letters, k=13)),
            "field29": "".join(random.choices(string.ascii_letters, k=13)),
            "field30": "".join(random.choices(string.ascii_letters, k=13)),
            "field31": "".join(random.choices(string.ascii_letters, k=13)),
            "field32": "".join(random.choices(string.ascii_letters, k=13)),
            "field33": "".join(random.choices(string.ascii_letters, k=13)),
            "field34": "".join(random.choices(string.ascii_letters, k=13)),
            "field35": "".join(random.choices(string.ascii_letters, k=13)),
            "field36": "".join(random.choices(string.ascii_letters, k=13)),
            "field37": "".join(random.choices(string.ascii_letters, k=13)),
            "field38": "".join(random.choices(string.ascii_letters, k=13)),
            "field39": "".join(random.choices(string.ascii_letters, k=13)),
            "field40": "".join(random.choices(string.ascii_letters, k=13)),
            "field41": "".join(random.choices(string.ascii_letters, k=13)),
            "field42": "".join(random.choices(string.ascii_letters, k=13)),
            "field43": "".join(random.choices(string.ascii_letters, k=13)),
            "field44": "".join(random.choices(string.ascii_letters, k=13)),
            "field45": "".join(random.choices(string.ascii_letters, k=13)),
            "field46": "".join(random.choices(string.ascii_letters, k=13)),
            "field47": "".join(random.choices(string.ascii_letters, k=13)),
            "field48": "".join(random.choices(string.ascii_letters, k=13)),
            "field49": "".join(random.choices(string.ascii_letters, k=13)),
            "field50": "".join(random.choices(string.ascii_letters, k=13)),
            "field51": "".join(random.choices(string.ascii_letters, k=13)),
            "field52": "".join(random.choices(string.ascii_letters, k=13)),
            "field53": "".join(random.choices(string.ascii_letters, k=13)),
            "field54": "".join(random.choices(string.ascii_letters, k=13)),
            "field55": "".join(random.choices(string.ascii_letters, k=13)),
            "field56": "".join(random.choices(string.ascii_letters, k=13)),
            "field57": "".join(random.choices(string.ascii_letters, k=13)),
            "field58": "".join(random.choices(string.ascii_letters, k=13)),
            "field59": "".join(random.choices(string.ascii_letters, k=13)),
            "field60": "".join(random.choices(string.ascii_letters, k=13)),
            "field61": "".join(random.choices(string.ascii_letters, k=13)),
            "field62": "".join(random.choices(string.ascii_letters, k=13)),
            "field63": "".join(random.choices(string.ascii_letters, k=13)),
            "field64": "".join(random.choices(string.ascii_letters, k=13)),
            "field65": "".join(random.choices(string.ascii_letters, k=13))
        })
        members = list(conn[common_variable.MONGODB_DB][common_variable.COLL_MEMBER].find({}, {"_id": 1}).limit(3000))
        coupon_slaves = []
        end_date = datetime.now() + timedelta(days=10)
        for member in members:
            for _ in range(3):
                coupon_slaves.append({
                    "coupon_id": coupon_id,
                    "end_date": end_date,
                    "member": {
                        "id": member["_id"],
                        "ts": datetime.now()
                    },
                    "status": "A",
                    "criteria": {
                        "use_limit": 3
                    },
                    "field1": "".join(random.choices(string.ascii_letters, k=13)),
                    "field2": "".join(random.choices(string.ascii_letters, k=13)),
                    "field3": "".join(random.choices(string.ascii_letters, k=13)),
                    "field4": "".join(random.choices(string.ascii_letters, k=13)),
                    "field5": "".join(random.choices(string.ascii_letters, k=13)),
                    "field6": "".join(random.choices(string.ascii_letters, k=13)),
                    "field7": "".join(random.choices(string.ascii_letters, k=13)),
                    "field8": "".join(random.choices(string.ascii_letters, k=13)),
                    "field9": "".join(random.choices(string.ascii_letters, k=13))
                })
        conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].insert_many(coupon_slaves, ordered=False)
        logging.info("Start Test Case 3-4 with coupon_id: "+str(coupon_id))
        environment.runner.send_message('coupon_id', coupon_id.binary)
    elif isinstance(environment.runner, WorkerRunner):
        environment.runner.register_message('coupon_id', load_coupon_id)
    else:
        logging.info("!!!")

class UseLimitUser(User):
    wait_time = constant(0)
    coupon_id = None

    def use_limit_coupon_slave(self, coupon_slave):
        coupon_slave_id = coupon_slave["_id"]
        use_limit = coupon_slave["criteria"]["use_limit"]
        update_status = "I"
        
        if use_limit == 1:
            update_status = "D"
        
        conn = mongo_client.get_conn()
        ## 搶使用
        result = conn[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].update_one(
            {
                "_id": coupon_slave_id
            }, {
                "$inc": {
                    "criteria.use_limit": -1
                },
                "$set": {
                    "status": update_status
                }
            }
        )
        if result.modified_count == 1:
            return "T"
        else:
            return "F"

    @task
    def run_use_limit_coupon_slave(self):
        member_id = None
        ## 查詢有此促銷券的會員
        result = mongo_client.get_conn()[common_variable.MONGODB_DB][common_variable.COLL_COUPON_SLAVE].find_one_and_update(
            {
                "coupon_id": self.coupon_id, 
                "status": { 
                    "$in": ["A", "I"] }
                ,"criteria.use_limit": {
                    "$gt": 0 } 
            }, 
            { "$set": { "status": "P" } }
        )
        if result:
            start_time = perf_counter_ns()
            result = self.use_limit_coupon_slave(result)
            duration = (perf_counter_ns() - start_time) / 1000000
            if result == "T":
                self.environment.events.request.fire(
                    request_type="GET", 
                    name="Use Coupon",
                    response_time=duration, 
                    response_length=0,
                    context=member_id,
                    exception=None
                )
            else:
                self.environment.events.request.fire(
                    request_type="GET", 
                    name="Use Coupon",
                    response_time=duration, 
                    response_length=0,
                    context=member_id,
                    exception="已達到總用量上限"
                )
        else:
            self.environment.events.request.fire(
                request_type="GET", 
                name="Use Coupon",
                response_time=0, 
                response_length=0,
                context=member_id,
                exception="無可用促銷券"
            )                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  