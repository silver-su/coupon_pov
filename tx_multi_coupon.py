"""
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
