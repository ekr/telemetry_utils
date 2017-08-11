import ujson as json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import plotly.plotly as py
import IPython
import hashlib
import struct

from __future__ import division
from moztelemetry.spark import get_one_ping_per_client, get_pings_properties
from moztelemetry import Dataset
from montecarlino import grouped_permutation_test


def find_test(x, test):
    for s in x["payload"]["tests"]:
        if s["website"] == test:
            return s
    return None

def succeeded(x) :
    if not "responseCode" in x["result"]:
        return "unknown"
    if x["result"]["responseCode"] != 0:
        return "success"
    else:
        return "fail"

def categorize(x):
    tls13 = succeeded(find_test(x, "enabled.tls13.com"))
    tls12 = succeeded(find_test(x, "control.tls12.com"))
    if tls13 == "unknown" or tls12 == "unknown":
        return "unknown"
    
    if tls13 == "success":
        if tls12 == "success":
            return "both_succeed"
        else:
            return "tls13_succeeds"
    else:
        if tls12 == "success":
            return "tls12_succeeds"
        else:
            return "both_fail"

def summarize(d):
    categorized = d.map(lambda x: categorize(x)).countByValue().items()
    df = pd.DataFrame(categorized, columns = ["Case", "Count"])
    df["Fraction"] = df["Count"] / sum(df["Count"])
    return df


def captive_portal(x):
    return x["payload"]["isNonBuiltInRootCertInstalled"]

def uses_captive_portal(x):
    return not find_test(x, "enabled.tls13.com")["isBuiltInRoot"]

def getSecurityState(d):
    try:
        return d["result"]["securityState"]
    except:
        return -1

def process(dataset, channel, begin, end):
    d = (dataset.where(docType='tls13-middlebox-repetition')
         .where(appName='Firefox')
         .where(appUpdateChannel=channel)
         .where(submissionDate=lambda x: x >= begin and x <= end))
    print "Got dataset filtered"
    logs = d.records(sc)
    print "Logs count ", logs.count()
    finished = logs.filter(lambda x: x["payload"]["status"] == "finished")
    print "Finished count ", finished.count()
    return [finished, summarize(finished)]


### STUFF TO RUN

dataset = Dataset.from_source('telemetry')
process(dataset, "beta", "20170701", "20170718")
process(dataset, "beta", "20170701", "20170806")
