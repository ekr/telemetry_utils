from moztelemetry import Dataset


# A filter that safely determines whether a ping has a histogram
# in it.
def filter_for_histogram(p, histogram):
    try:
        if p[histogram] is None:
            return False
        return p[histogram].sum() > 0
    except:
        return False

# Convenience function to turn a histogram name into the payload
# name.
def payload(h):
    return "payload/histograms/%s"%h


# Count the number of reports which have a given histogram in them.
def count_reports(sc, pings, h):
    histogram = payload(h)
    reduced = pings.filter(lambda p: filter_for_histogram(p, histogram))
    accum = sc.accumulator(0)
    reduced.foreach(lambda p: accum.add(p[histogram].sum()))
    return accum

# Compare the number of counts between two sets of pings for a given
# histogram.
def compare_counts(sc, pings, a, b):
    ca = count_reports(sc, pings, a).value
    cb = count_reports(sc, pings, b).value
    print ca, cb, ca/cb
    return [ca, cb]

# Internal helper. Do not use.
def accum_histogram(accums, hist):
    for i in range(0, len(hist)):
        accums[i].add(hist[i])

# Takes a set of pings and then sum up all the buckets of a given
# histogram, returning a python dict mapping bucket : count. The
# buckets argument tells you how many histogram buckets there are
# so we can make accumulators. It's fine to go over, but going under
# will fail. This may not be ideal.
#
# Example call: utils.sum_histogram(sc, nightly_pings, 800, "SSL_HANDSHAKE_RESULT")
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

# Internal helper. Do not use.
def accum_histogram_experiment(accums, p, arm_func, histogram):
    acc = accums[arm_func(p)]
    accum_histogram(acc, p[histogram])

# Comparison function for counts for a given experiment.
# Takes a set of pings, plus a function that tells you whether
# a ping is in the treatment or control group and returns
# a dict containing the bucket : count mappings for each
# experimental arm.
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

# Internal helper. Do not use.
def get_value(h, key, s):
    if not key in h:
        return [0, "-"]
    else:
        return [h[key], h[key]/s]

# Takes a dict of the form output by sum_histogram_experiment and
# turns it into a contingency table. Specifically:
#
# - Compute fractions for each histogram bucket.
# - Sort by frequency (in the control group) with highest frequency
#   last.  
# - If a non-empty table argument is provided, attach labels to
#   each bucket.
#
# The result is returned as an array with one row for each
# bucket, so you can get the most frequent ones with [-X:]
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

# Wrapper function to run compare_branches_proportions for > 1
# histogram. This just lets you kick off a lot of long analyses
# and walk away. The return value is just a dict of the individual
# result keyed by histogram name.
#
# sc: the spark context
# pings: the pings to sample
# histograms: a list of histograms to do
# arm_func: returns which experimental arm you are in.
# trans: the bucket to name translation tables, keyed by histogram
def run_comparison_panel(sc, pings, histograms, arm_func, trans):
    res = {}
    for h in histograms:
        t = {}
        if trans is not None and h in trans:
            t = trans[h]
        r = sum_histogram_experiment(sc, pings, 1000, h, arm_func)
        res[h] = compare_branches_proportions(r, t)
    return res


# Normalizes a set of reports by client. The specific procedure is:
#
# - Group by client
# - Normalize each histogram bucket value by the total number of
#   counts across all buckets
#
# This procedure roughly lets you remove very high weight outliers.
# You can feed the result into the functions above.
def merge_normalize_client_reports(sc, pings, h):
    histogram = payload(h)
    reduced = pings.filter(lambda p: filter_for_histogram(p, histogram))
    grouped = reduced.map(lambda x: (x['clientId'], x[histogram])).groupByKey()
    aggregated = grouped.map(lambda x: (x[0], reduce(lambda a,b: a+b, x[1])))
    normalized = aggregated.map(lambda x: { "clientId": x[0], histogram: x[1]/sum(x[1])})
    return normalized

# The same as sum_histogram_experiment but normalizes by client first.
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

# The same as run_comparison_panel but normalizes by client first.
def run_comparison_panel_by_client(sc, pings, histograms, arm_func, trans):
    res = {}
    for h in histograms:
        t = {}
        if trans is not None and h in trans:
            t = trans[h]
        r = sum_histogram_experiment_by_client(sc, pings, 1000, h, arm_func)
        res[h] = compare_branches_proportions(r, t)
    return res

# Render a contingency table in a nicer way, hopefully suitable for
# publication.
def render_compared_histogram(d):
    format = row_format ="{:<40} {:>20} {:>20}"
    print format.format("", "Control", "Treatment")
    for j in d:
        print format.format(j[0], j[1][1], j[2][1])
        

# A filter to sample by clientId.
#
# x: the ping
# label: a label to use for the sample function. By using
#        different labels, you get different pings sampled
# frac:  the fraction of clients to sample
#        
def sample_by_client_id(x, label, frac):
    h = hashlib.sha256(x["clientId"] + label)
    v = (struct.unpack(">L", h.digest()[0:4])[0])
    variate = v/ 0xffffffff
    if variate < frac:
        return True
    else:
        return False
