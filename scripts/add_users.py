#!/bin/python2

from datetime import datetime

import os
import argparse
import csv
import pymongo
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cleaner.py'))

from cleaner import UserDataCleaner

WHITELIST = ['name', 'email', 'title', 'organization', 'source',
             'interested_topics', 'expectation', 'interested_in_volunteer',
             'location', 'linkedin', 'github', 'resume', 'feedback', 'subscription_status']
ARRAY_FIELDS = ['source', 'interested_topics', 'expectation']

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
            delim = ',' if filename.endswith(".csv") else '\t'
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

def transform_one(user, cleaner):
    result = {}
    for k, v in user.iteritems():
        if (not v) or (v.lower() == 'n/a'):
            continue
        (k, v) = cleaner.clean(k, v)
        result[k] = v
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
    cleaner = UserDataCleaner(fields)
    for user in users:
        transformed = transform_one(user, cleaner)
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
    #        print("\t\tQuereid: {}\n\t\tNew: {}\n\t\tUpdate: {}".format(user, user_data, update))
            if not dryrun:
                coll.update_one({'_id': user['_id']}, {"$set": update}, upsert = False)
    else:
        user_data = set_default(user_data)
    #    print("Adding {}".format(user_data))
        if not dryrun:
            coll.insert_one(user_data)

def run(args):
    users = parse_users(args.datadir)
    default_field_map = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'field_map.csv')
    fields = parse_fields(args.fields or default_field_map)
    add_all(users, fields, args.dryrun)

def main():
    run(parse_args())

if __name__ == '__main__':
    main()
