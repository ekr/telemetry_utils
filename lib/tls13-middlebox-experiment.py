
# coding: utf-8

# In[20]:

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

DISABLED_FP = "55:45:DD:2D:5A:C8:E4:55:8A:F4:09:62:5A:2D:45:0A:85:17:0D:6F:F1:BF:3A:01:14:13:88:7F:CA:E3:4A:DF"
CONTROL_FP = "65:94:4C:F6:80:BF:1B:1B:80:29:24:E8:EF:6D:B5:92:74:BD:A8:87:3F:48:0B:C5:B7:A1:0C:02:3C:5C:DB:04"
ENABLED_FP = "93:2B:65:33:96:B3:E3:05:5A:42:D8:EF:CA:C1:04:3D:E0:C9:FD:41:F0:AA:EC:7C:3F:58:DB:E8:17:9B:22:9C"

def isSucceeded(x):
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


def isNonBuiltInRootCertInstalled(x):
    if "isNonBuiltInRootCertInstalled" in x["payload"]:
        return x["payload"]["isNonBuiltInRootCertInstalled"]
    else:
        return None

def isBuiltInRoot(x):
    return not find_test(x, "enabled.tls13.com")["isBuiltInRoot"]

def getSecurityState(d):
    try:
        return d["result"]["securityState"]
    except:
        return -1

def fetchLogs(channel, begin, end):
    dataset = Dataset.from_source('telemetry')

    d = (dataset.where(docType="tls13-middlebox-repetition")
                .where(appName="Firefox")
                .where(appUpdateChannel=channel)
                .where(submissionDate=lambda x: x >= begin and x <= end))

    logs = d.records(sc)

    return logs

def getTestByWebsite(log, website):
    for t in log["payload"]["tests"]:
        if t["website"] == website:
            return t

    return None

def analyzeNonBuiltInRootCerts(logs):
    aborted_finished_logs = logs.filter(lambda x: x["payload"]["status"] in ["aborted", "finished"])
    nonbuiltin_root_cert = aborted_finished_logs.map(lambda x: isNonBuiltInRootCertInstalled(x)).countByValue()
    
    print "isNonBuiltInRootCertInstalled: ", json.dumps(nonbuiltin_root_cert)

def countLogs(logs):
    logs_status = logs.map(lambda x: x["payload"]["status"]).countByValue()
    
    logs_status["total"] = logs.count()
    
    aborted_count = logs_status["aborted"] if "aborted" in logs_status else 0
    
    logs_status["failure"] = logs_status["started"] - (aborted_count + logs_status["finished"])
    
    print "Count: ", json.dumps(logs_status)

if __name__ == "__main__":
    logs = fetchLogs("nightly", "20170701", "20170901")
    
    countLogs(logs)
    analyzeNonBuiltInRootCerts(logs)


# In[ ]:




# In[ ]:



