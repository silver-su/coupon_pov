"""
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
            )