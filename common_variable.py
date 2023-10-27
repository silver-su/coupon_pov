from pymongo import MongoClient, WriteConcern
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
    })