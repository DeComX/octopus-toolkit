import pymongo
import bcrypt
import argparse

def get_coll():
    client = pymongo.MongoClient("mongodb://root:root@127.0.0.1:27017/dev?authSource=admin")
    db = client.get_database()
    return db['users']

def parse_args():
    parser = argparse.ArgumentParser(description='register new user')
    parser.add_argument('--name', dest='name', help='user name to register')
    parser.add_argument('--email', dest='email', help='email to register')
    parser.add_argument('--password', dest='password', help='password to register')
    args = parser.parse_args()
    return args

def run(args):
    users = get_coll()
    user = users.find_one({"email": args.email})
    if user:
        print("Email has been registered")
        return
    data = {
        "name": args.name,
        "email": args.email,
        "password": args.password
    }
    salt = bcrypt.gensalt(10)
    data['password'] = bcrypt.hashpw(data['password'].encode('utf-8'), salt)
    users.insert_one(data)
    print("User added")

def main():
    run(parse_args())

if __name__ == '__main__':
    main()
