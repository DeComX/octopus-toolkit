#!/bin/python2

from datetime import datetime

import argparse
import csv
import os
import pymongo

WHITELIST = ['name', 'email', 'title', 'organization', 'source',
             'interested_topics', 'expectation', 'interested_in_volunteer',
             'location', 'linkedin', 'github', 'resume', 'feedback', 'subscription_status']
ARRAY_FIELDS = ['source', 'interested_topics', 'expectation']
CANONICAL_FIELDS = ['title', 'location', 'source']
BOOLEAN_FIELDS = ['interested_in_volunteer']
SNAKE_CASE_FIELDS = ['source', 'interested_topics']

def get_coll():
    client = pymongo.MongoClient("mongodb://root:root@127.0.0.1:27017/dev?authSource=admin")
    db = client.get_database()
    return db['user']

def parse_args():
    parser = argparse.ArgumentParser(description='import users')
    parser.add_argument('--datadir', dest='datadir', help='dir with user data')
    parser.add_argument('--fields', dest='fields', help='fields map')
    parser.add_argument('--dryrun', dest='dryrun', action='store_true')
    args = parser.parse_args()
    return args

def parse_users(directory):
    for filename in os.listdir(directory):
        if filename.endswith(".csv") or filename.endswith(".tsv"):
            delim = ',' if filename.endswith(".csv") == 'csv' else '\t'
            fullname = os.path.join(directory, filename)
            print('Processing file {}'.format(fullname))
            with open(fullname, 'r') as f:
                users = csv.DictReader(f, delimiter=delim)
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
    value = value.lower().strip()
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        value = value[1:-1]
    value = value.replace('&', ' and ')
    value = value.replace('/', ' and ')
    return ' '.join(value.split())

def to_snake_case(value):
    return "_".join(value.lower().strip().split(' '))

def transform_one(user, fields):
    result = {}
    for k, v in user.iteritems():
        if (not v) or (v.lower() == 'n/a'):
            continue

        k = canonicalize(k)
        field_name = fields.get(k, k)
        if k in CANONICAL_FIELDS:
            v = canonicalize(v)

        if field_name in ARRAY_FIELDS:
            v = [i.strip() for i in v.split(',')]
            if field_name in SNAKE_CASE_FIELDS:
                v = [to_snake_case(i) for i in v]
        else:
            if field_name in SNAKE_CASE_FIELDS:
                v = to_snake_case(v)
            if field_name in BOOLEAN_FIELDS:
                v = v.lower() in ['yes', 'y', 'true']

        result[field_name] = v
    return result

def post_process(user):
    if "name" not in user and ("first_name" in user and "last_name" in user):
        user['name'] = user["first_name"] + " " + user["last_name"]
    result = {}
    for k, v in user.iteritems():
        if k in WHITELIST:
            result[k] = v
    return result

def add_all(users, fields, dryrun):
    for user in users:
        transformed = transform_one(user, fields)
        processed = post_process(transformed)
        add_one(processed, dryrun)

def get_update(queried, new):
    update = {}
    for k, v in new.iteritems():
        if v and (k not in queried or queried[k] != v):
            if k in ARRAY_FIELDS:
                update[k] = list(set((queried[k] if k in queried else []) + v))
            else:
                update[k] = v
    return update

def set_default(user):
    if 'name' not in user or not user['name']:
        user['name'] = 'unknown'
    if 'interested_in_volunteer' not in user:
        user['interested_in_volunteer'] = False
    if 'subscription_status' not in user:
        user['subscription_status'] = 'unsubscribed'
    return user

def add_one(user_data, dryrun):
    if ('email' not in user_data) or (not user_data['email']):
        print("Missing email field for record {}".format(user_data))
        return

    coll = get_coll()
    user = coll.find_one({"email": user_data['email']})
    if user:
        update = get_update(user, user_data)

        if update:
            print("\t\tQuereid: {}\n\t\tNew: {}\n\t\tUpdate: {}".format(user, user_data, update))
            if not dryrun:
                coll.update_one({'_id': user['_id']}, {"$set": update}, upsert = False)
    else:
        user_data = set_default(user_data)
        print("Adding {}".format(user_data))
        if not dryrun:
            coll.insert_one(user_data)

def run(args):
    users = parse_users(args.datadir)
    fields = parse_fields(args.fields)
    add_all(users, fields, args.dryrun)

def main():
    run(parse_args())

if __name__ == '__main__':
    main()
