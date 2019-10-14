#!/bin/python2

import re

def canonicalize_title(title):
    tocheck = title.lower().strip()
    if tocheck == 'cto':
        return 'CTO'
    elif tocheck == 'ceo':
        return 'CEO'
    elif tocheck == 'cmo':
        return 'CMO'
    elif tocheck in ['cofounder', 'co-founder', 'co founder']:
        return 'Co-Founder'
    return title

class Cleaner:
    def __init__(self, field_map):
        self.field_map = field_map

    def clean(self, key, value):
        key = self.preclean(key).lower()
        key = self.field_map[key] if key in self.field_map else key
        value = self.preclean(value)
        return self.do_clean(key, value)

    def do_clean(self, key, value):
        raise 'Not Implemented Exception'

    def preclean(self, value):
        value = value.strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        return value.strip()

    def to_snake_case(self, value):
        return "_".join(value.strip().split(' '))

class UserDataCleaner(Cleaner):
    def do_clean(self, key, value):
        processed = value
        if key == 'source':
            processed = self.clean_source(value)
        elif key == 'title':
            processed = self.clean_title(value)
        elif key == 'interested_topics':
            processed = self.clean_interested_topics(value)
        elif key == 'expectation':
            processed = self.clean_expectation(value)
        elif key == 'interested_in_volunteer':
            processed = self.clean_interested_in_volunteer(value)
            print("interested_in_volunteer {} ------ {}".format(value, processed))
        return (key, processed)

    def clean_source(self, value):
        sources = re.split(r'[&/,|]| and ', value.lower())
        return [self.to_snake_case(s.strip()) for s in sources]

    def clean_title(self, title):
        titles = re.split(r'[&/,|]| and ', title)
        titles = [canonicalize_title(t) for t in titles]
        return ','.join(titles)

    def clean_interested_topics(self, value):
        topics = re.split(r'[&/,|]| and ', value)
        return [t.lower().strip() for t in topics]

    def clean_expectation(self, value):
        expectations = re.split(r'[&/,|]| and ', value)
        return [e.strip() for e in expectations]

    def clean_interested_in_volunteer(self, value):
        value = value.lower()
        return value in ['yes', 'y', 'true'] or value.startswith('yes')
