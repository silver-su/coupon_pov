"""
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
