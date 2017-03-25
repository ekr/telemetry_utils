from moztelemetry import Dataset

def is_in_tls_experiment(ping):
    try:
        experiment = ping["environment"]["addons"]["activeExperiment"]
        return experiment["id"] == "tls13-compat-ff51@experiments.mozilla.org"
    except:
        return False

def is_not_in_tls_experiment(ping):
    try:
        experiment = ping["environment"]["addons"]["activeExperiment"]
    except:
        return False
    try:
        return experiment["id"] != "tls13-compat-ff51@experiments.mozilla.org"
    except:
        return True

def filter_for_histogram(p, histogram):
    try:
        if p[histogram] is None:
            return False
        return p[histogram].sum() > 0
    except:
        return False

def payload(h):
    return "payload/histograms/%s"%h

def count_reports(sc, pings, h):
    histogram = payload(h)
    reduced = pings.filter(lambda p: filter_for_histogram(p, histogram))
    accum = sc.accumulator(0)
    reduced.foreach(lambda p: accum.add(p[histogram].sum()))
    return accum

def compare_counts(sc, pings, a, b):
    ca = count_reports(sc, pings, a).value
    cb = count_reports(sc, pings, b).value
    print ca, cb, ca/cb
    return [ca, cb]
    
def accum_histogram(accums, hist):
    for i in range(0, len(hist)):
        accums[i].add(hist[i])


def sum_histogram(sc, pings, buckets, h):
    histogram = payload(h)
    reduced = pings.filter(lambda p: filter_for_histogram(p, histogram))
    accums = []
    for i in range(0, buckets):
        accums.append(sc.accumulator(0))
    reduced.foreach(lambda p: accum_histogram(accums, p[histogram]))
    res = {}
    for i in range(0, buckets):
        if accums[i].value != 0:
            res[i] = accums[i].value
    return res

def get_pings_by_version(sc, channel, vernum):
    p = (Dataset.from_source('telemetry-sample')
         .where(docType='main')
         .where(appName='Firefox')
         .where(appUpdateChannel=channel)
         .where(appVersion=lambda x: x >= "%d."%vernum and x < "%d."%(vernum+1))
         .records(sc))
    return get_pings_properties(p, properties_to_gather)
    
