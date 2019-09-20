import pymongo
from datetime import datetime

client = pymongo.MongoClient("mongodb://root:root@127.0.0.1:27017/dev?authSource=admin")
db = client.get_database()
member = db['members']
email_postfix = ["gmail.com", "abcer.world", "tj.edu"]
for i in range(1, 400):
    data = {
        "name": "member_{}".format(i % 380),
        "email": "e_{}@{}".format(i, email_postfix[i % 3]),
        "title": "title_{}".format(i % 20),
        "organization": "org_{}".format(i % 100),
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "description": "blablablabee"
    }
    member.insert_one(data)
