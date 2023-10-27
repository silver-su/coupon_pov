"""
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
            )