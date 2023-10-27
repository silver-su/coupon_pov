from pymongo import MongoClient
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
