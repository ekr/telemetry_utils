import json
from moztelemetry import Dataset

DISABLED_FP = "55:45:DD:2D:5A:C8:E4:55:8A:F4:09:62:5A:2D:45:0A:85:17:0D:6F:F1:BF:3A:01:14:13:88:7F:CA:E3:4A:DF"
CONTROL_FP = "65:94:4C:F6:80:BF:1B:1B:80:29:24:E8:EF:6D:B5:92:74:BD:A8:87:3F:48:0B:C5:B7:A1:0C:02:3C:5C:DB:04"
ENABLED_FP = "93:2B:65:33:96:B3:E3:05:5A:42:D8:EF:CA:C1:04:3D:E0:C9:FD:41:F0:AA:EC:7C:3F:58:DB:E8:17:9B:22:9C"

ENABLED_WEBSITE = "https://enabled.tls13.com"
DISABLED_WEBSITE = "https://disabled.tls13.com"
CONTROL_WEBSITE = "https://control.tls12.com"
NO_TLS_WEBSITE = "http://tls12.com"

WEBSITES = [
    ENABLED_WEBSITE,
    DISABLED_WEBSITE,
    CONTROL_WEBSITE,
    NO_TLS_WEBSITE
]

error_messages = {}

def translateError(status, error_code):
    if status in [0, None] and error_code in [0, None]:
        return None
    
    msg = []
    
    if status != 0 and status in error_messages:
        msg.extend(error_messages[status])

    if error_code != 0 and error_code in error_messages:
        for m in error_messages[error_code]:
            if m not in msg:
                msg.append(m)

    return ','.join(msg)

def findTestByWebsite(x, website):
    for t in x["payload"]["tests"]:
        if t["website"] == website:
            return t
        
    return None

def filterLogsByStatus(logs, status_list):
    return logs.filter(lambda x: x["payload"]["status"] in status_list)

def analyzeSuccess(logs, successCriteria):
    def categorizeSuccess(x):
        tls13_enabled = successCriteria(findTestByWebsite(x, ENABLED_WEBSITE))
        tls13_disabled = successCriteria(findTestByWebsite(x, DISABLED_WEBSITE))

        if tls13_enabled is None or tls13_disabled is None:
            return "unknown"

        if tls13_enabled:
            if tls13_disabled:
                return "Both Succeeded"
            else:
                return "Only TLS 1.3 Succeeded"
        else:
            if tls13_disabled:
                return "Only TLS 1.2 Succeeded"
            else:
                return "Both Failed"

    finished_logs = filterLogsByStatus(logs, ["finished"])
    
    success = finished_logs.map(lambda x: categorizeSuccess(x)).countByValue()
    
    total = sum(success.values())
    
    for k in success:
        success[k] = "%d (%.1f%%)" % (success[k], success[k] * 100.0 / total)
    
    print "Success: %s\n\n" % jsonToString(success)

def analyzeErrors(logs):
    def categorizeError(x):
        res = set()
        
        for test in x["payload"]["tests"]:
            for result in test["results"]:
                status = result["status"] if "status" in result else None
                error_code = result["errorCode"] if "errorCode" in result else None

                res.add(translateError(status, error_code))
                
        res.discard(None)
        
        return list(res)

    finished_logs = filterLogsByStatus(logs, ["finished"])
    
    errors = finished_logs.flatMap(lambda x: categorizeError(x)).countByValue()
    
    print "Errors: %s\n\n" % jsonToString(errors)

def isNonBuiltInRootCertInstalled(x):
    if "isNonBuiltInRootCertInstalled" in x["payload"]:
        return x["payload"]["isNonBuiltInRootCertInstalled"]
    else:
        return None

def analyzeNonBuiltInRootCerts(logs):
    aborted_finished_logs = filterLogsByStatus(logs, ["aborted", "finished"])
    nonbuiltin_root_cert = aborted_finished_logs.map(lambda x: isNonBuiltInRootCertInstalled(x)).countByValue()
    
    print "Installed non-builtin root cert: %d (%.1f%%)\n\n" % (                    nonbuiltin_root_cert[True],
                    nonbuiltin_root_cert[True] * 100.0 / sum(nonbuiltin_root_cert.values()))

def analyzeCount(logs):
    logs_status = logs.map(lambda x: x["payload"]["status"]).countByValue()
    
    logs_status["total"] = logs.count()
    
    aborted_count = logs_status["aborted"] if "aborted" in logs_status else 0
    
    logs_status["failed"] = logs_status["started"] - (aborted_count + logs_status["finished"])
        
    for k in logs_status:
        if k != "total":
            logs_status[k] = "%d (%.1f%%)" % (logs_status[k], logs_status[k] * 100.0 / logs_status["total"])
    
    print "Count: %s\n\n" % jsonToString(logs_status)

def fetchLogs(channel, begin, end):
    dataset = Dataset.from_source('telemetry')

    d = (dataset.where(docType="tls13-middlebox-alt-server-hello-1")
                .where(appName="Firefox")
                .where(appUpdateChannel=channel)
                .where(submissionDate=lambda x: x >= begin and x <= end))

    logs = d.records(sc)

    return logs

def jsonToString(data):
    return json.dumps(data, indent=4, separators=(',', ': '))

def successCriteriaAtLeastOne(test):
    if test is None:
        return None
    
    for r in test["results"]:
        if r["event"] in ["load", "loadend"]:
            return True

    return False

def successCriteriaFirstOne(test):
    if test is None:
        return None
    
    if len(test["results"]) > 0:
        if test["results"][0]["event"] in ["load", "loadend"]:
            return True

    return False


def findTestByLabel(x, label):
    for t in x["payload"]["tests"]:
        if t["label"] == label:
            return t
        
    return None

def rawCountSuccess(logs, successCriteria, label):
    finished_logs = filterLogsByStatus(logs, ["finished"])
    
    success = finished_logs.map(lambda x: successCriteria(findTestByLabel(x, label))).countByValue()
    
    total = sum(success.values())
    
    for k in success:
        success[k] = "%d (%.1f%%)" % (success[k], success[k] * 100.0 / total)
    
    print "Success: %s\n\n" % jsonToString(success)


    

    

if __name__ == "__main__":
    # load error codes and their descriptions
    with open("error_types.txt", "r") as f:
        for line in f:
            tokens = line.strip().split()

            if int(tokens[0], 16) not in error_messages:
                error_messages[int(tokens[0], 16)] = []

            error_messages[int(tokens[0], 16)].append(tokens[1])

    # fetch all the logs from a channel
    logs = fetchLogs("nightly", "20170701", "20170901")

    analyzeCount(logs)
    analyzeNonBuiltInRootCerts(logs)
    analyzeErrors(logs)

    print "------- Analysis with at least one success out of 5 tries"
    analyzeSuccess(logs, successCriteriaAtLeastOne)

    print "------- Analysis with the first successful try"
    analyzeSuccess(logs, successCriteriaFirstOne)


