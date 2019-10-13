from datetime import datetime

import argparse
import csv
import pymongo

SKIPPED_FEILDS = ['experience', 'timestamp']
ARRARY_FIELDS = ['source', 'interested_topics', 'expectation']
BOOLEAN_FIELDS = ['interested_in_volunteer']
CANONICAL_FIELDS = ['source', 'interested_topics']

def get_coll():
    client = pymongo.MongoClient("mongodb://root:root@127.0.0.1:27017/dev?authSource=admin")
    db = client.get_database()
    return db['user']

def parse_args():
    parser = argparse.ArgumentParser(description='import users')
    parser.add_argument('--users', dest='users', help='file with user data')
    parser.add_argument('--fields', dest='fields', help='fields map')
    args = parser.parse_args()
    return args

def parse_users(filename):
    print('Parsing users data')
    with open(filename, 'r') as tsvfile:
        users = csv.DictReader(tsvfile, delimiter='\t')
        for user in users:
            yield user

def parse_fields(filename):
    fields = {}
    with open(filename, 'r') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            fields[row[0]] = row[1]
    return fields

def canonicalize(value):
    value = value.replace('&', 'and')
    return ('_').join(value.lower().split(' '))

def transform_one(user_data, fields):
    result = {}
    for k,v in user_data.iteritems():
        k = k.lower().strip()
        v = v.strip()
        field_name = fields.get(k, k)

        if (not v) or (v.lower() == 'n/a') or (field_name in SKIPPED_FEILDS):
            continue

        if field_name in ARRARY_FIELDS:
            v = [i.strip() for i in v.split(',')]
            if field_name in CANONICAL_FIELDS:
                v = [canonicalize(i) for i in v]
        else:
            if field_name in CANONICAL_FIELDS:
                v = canonicalize(v)
            if field_name in BOOLEAN_FIELDS:
                v = v.lower() in ['yes', 'y', 'true']

        result[field_name] = v

    return result

def add_all(users, fields):
    for user in users:
        add_one(transform_one(user, fields))

def merge(queried, new):
    for k, v in new.iteritems():
        if k not in queried or queried[k] != v:
            queried[k] = v
    return queried

def add_one(user_data):
    if ('email' not in user_data) or (not user_data['email']):
        print("Missing email field for record {}".format(user_data))
        return

    coll = get_coll()
    user = coll.find_one({"email": user_data['email']})
    if user:
        print("Found user with email {}.".format(user_data['email']))
        merged = merge(user, user_data)
        print("\t\tQuereid: {}\n\t\tNew: {}\n\t\tMerged record: {}".format(user, user_data, merged))
#        coll.update_one(merged)
    else:
        print("Adding {}".format(user_data))
#        coll.insert_one(user_data)

def run(args):
    users = parse_users(args.users)
    fields = parse_fields(args.fields)
    add_all(users, fields)

def main():
    run(parse_args())

if __name__ == '__main__':
    main()
