"""
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
                )