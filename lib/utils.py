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

def accum_histogram_experiment(accums, p, arm_func, histogram):
    acc = accums[arm_func(p)]
    accum_histogram(acc, p[histogram])
    
def sum_histogram_experiment(sc, pings, buckets, h, arm_func):
    histogram = payload(h)
    reduced = pings.filter(lambda p: filter_for_histogram(p, histogram))
    accums = {}
    arms = ["control", "treatment"]
    for a in arms:
        accums[a] = []
        for i in range(0, buckets):
            accums[a].append(sc.accumulator(0))
    reduced.foreach(lambda p: accum_histogram_experiment(accums, p, arm_func, histogram))
    res = {}
    for a in arms:
        res[a] = {}
        for i in range(0, buckets):
            if accums[a][i].value != 0:
                res[a][i] = accums[a][i].value
    return res

def get_pings_by_version(sc, channel, vernum):
    p = (Dataset.from_source('telemetry-sample')
         .where(docType='main')
         .where(appName='Firefox')
         .where(appUpdateChannel=channel)
         .where(appVersion=lambda x: x >= "%d."%vernum and x < "%d."%(vernum+1))
         .records(sc))
    return get_pings_properties(p, properties_to_gather)
    

def get_value(h, key, s):
    if not key in h:
        return [0, "-"]
    else:
        return [h[key], h[key]/s]

def compare_branches_proportions(inp, table):
    a = inp['control']
    b = inp['treatment']
    res = []
    keys = set().union(a.keys(), b.keys())
    suma = sum([a[k] for k in a])
    sumb = sum([b[k] for k in b])
    for k in keys:
        n = "%d"%k
        if k in table:
            n = table[k]
        va = get_value(a, k, suma)
        vb = get_value(b, k, sumb)
        res.append([n, va, vb])
    res = sorted(res, key=lambda p: p[1])
    return res

    
def run_comparison_panel(sc, pings, histograms, arm_func, trans):
    res = {}
    for h in histograms:
        t = {}
        if trans is not None and h in trans:
            t = trans[h]
        r = sum_histogram_experiment(sc, pings, 1000, h, arm_func)
        res[h] = compare_branches_proportions(r, t)
    return res

def merge_normalize_client_reports(sc, pings, h):
    histogram = payload(h)
    reduced = pings.filter(lambda p: filter_for_histogram(p, histogram))
    grouped = reduced.map(lambda x: (x['clientId'], x[histogram])).groupByKey()
    aggregated = grouped.map(lambda x: (x[0], reduce(lambda a,b: a+b, x[1])))
    normalized = aggregated.map(lambda x: { "clientId": x[0], histogram: x[1]/sum(x[1])})
    return normalized
                                                                                     
def sum_histogram_experiment_by_client(sc, pings, buckets, h, arm_func):
    histogram = payload(h)
    accums = {}
    arms = ["control", "treatment"]
    for a in arms:
        accums[a] = []
        for i in range(0, buckets):
            accums[a].append(sc.accumulator(0))
    merged = merge_normalize_client_reports(sc, pings, h)
    merged.foreach(lambda p: accum_histogram_experiment(accums, p, arm_func, histogram))
    res = {}
    for a in arms:
        res[a] = {}
        for i in range(0, buckets):
            if accums[a][i].value != 0:
                res[a][i] = accums[a][i].value
    return res

def run_comparison_panel_by_client(sc, pings, histograms, arm_func, trans):
    res = {}
    for h in histograms:
        t = {}
        if trans is not None and h in trans:
            t = trans[h]
        r = sum_histogram_experiment_by_client(sc, pings, 1000, h, arm_func)
        res[h] = compare_branches_proportions(r, t)
    return res

def render_compared_histogram(d):
    format = row_format ="{:<40} {:>20} {:>20}"
    print format.format("", "Control", "Treatment")
    for j in d:
        print format.format(j[0], j[1][1], j[2][1])
        

def sample_by_client_id(x, label, frac):
    h = hashlib.sha256(x["clientId"] + label)
    v = (struct.unpack(">L", h.digest()[0:4])[0])
    variate = v/ 0xffffffff
    if variate < frac:
        return True
    else:
        return False
