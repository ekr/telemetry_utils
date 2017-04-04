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

%pylab inline
IPython.core.pylabtools.figsize(16, 7)
import warnings; warnings.simplefilter('ignore')


!rm -rf telemetry_utils && git clone https://github.com/ekr/telemetry_utils
sc.addPyFile("telemetry_utils/lib/utils.py")
import utils
sc.addPyFile("telemetry_utils/lib/tls.py")
import tls

properties_to_gather=[
    utils.payload("SSL_HANDSHAKE_VERSION"),
    utils.payload("SSL_TLS12_INTOLERANCE_REASON_PRE"),
    utils.payload("SSL_HANDSHAKE_RESULT"),
    utils.payload("SSL_TLS13_INTOLERANCE_REASON_PRE"),
    utils.payload("HTTP_CHANNEL_DISPOSITION"),
    "clientId"]

def predict_arm(x):
    h = hashlib.sha256(x["clientId"] + "tls13-comparison-all-v1@mozilla.org")
    v = (struct.unpack(">L", h.digest()[0:4])[0])
    variate = v/ 0xffffffff
    if variate < 0.5:
        return "treatment"
    else:
        return "control"

beta53_pings_full = (Dataset.from_source('telemetry')
                .where(docType='main')
                .where(appName='Firefox')
                .where(appUpdateChannel='beta')
                .where(appVersion=lambda x: x >= "53." and x < "54.")
                .where(submissionDate=lambda x: x >= '20170323' and x <= '2017331')
                .records(sc))
beta53_exp_pings_full = get_pings_properties(beta53_pings_full.filter(in_experiment), properties_to_gather)
beta53_exp_pings_full.cache()


beta53_pings_sample = (Dataset.from_source('telemetry-sample')
                .where(docType='main')
                .where(appName='Firefox')
                .where(appUpdateChannel='beta')
                .where(appVersion=lambda x: x >= "53." and x < "54.")
                .where(submissionDate=lambda x: x >= '20170323' and x <= '2017331')
                .records(sc))
beta53_exp_pings_sample = get_pings_properties(beta53_pings_sample.filter(in_experiment), properties_to_gather)
beta53_exp_pings_sample.cache()


beta53_pings_old = (Dataset.from_source('telemetry')
                .where(docType='main')
                .where(appName='Firefox')
                .where(appUpdateChannel='beta')
                .where(appVersion=lambda x: x >= "53." and x < "54.")
                .where(submissionDate=lambda x: x >= '20170310' and x <= '2017320')
                .records(sc))
beta53_all_pings_old = get_pings_properties(beta53_pings_old, properties_to_gather)
beta53_all_pings_old.cache()


sample_results = utils.run_comparison_panel(sc, beta53_exp_pings_sample, histograms, predict_arm, tls.HISTOGRAM_LABELS)
full_results = utils.run_comparison_panel(sc, beta53_exp_pings_full, histograms, predict_arm, tls.HISTOGRAM_LABELS)






nightly_pings = (Dataset.from_source('telemetry')
                .where(docType='main')
                .where(appName='Firefox')
                .where(appUpdateChannel='nightly')
                .where(appBuildId=lambda x: x >= "20170215000000")
                .records(sc, sample=1))
nightly_pings = get_pings_properties(nightly_pings, properties_to_gather)

x = utils.sum_histogram(sc, nightly_pings, 800, "SSL_HANDSHAKE_RESULT")
tls.translate_errors(x)


beta50_pings = (Dataset.from_source('telemetry-sample')
                .where(docType='main')
                .where(appName='Firefox')
                .where(appUpdateChannel='beta')
                .where(appVersion=lambda x: x >= "50." and x < "51.")
                .records(sc))
beta50_pings = get_pings_properties(beta50_pings, properties_to_gather)
res50 = utils.compare_counts(sc, beta50_pings, "SSL_TLS12_INTOLERANCE_REASON_PRE", "SSL_HANDSHAKE_VERSION")
print res50


beta52_pings = (Dataset.from_source('telemetry-sample')
                .where(docType='main')
                .where(appName='Firefox')
                .where(appUpdateChannel='beta')
                .where(appVersion=lambda x: x >= "52." and x < "53.")
                .records(sc))
beta52_pings = get_pings_properties(beta50_pings, properties_to_gather)
res52 = utils.compare_counts(sc, beta50_pings, "SSL_TLS13_INTOLERANCE_REASON_PRE", "SSL_HANDSHAKE_VERSION")
print res52


release52_pings = (Dataset.from_source('telemetry-sample')
                .where(docType='main')
                .where(appName='Firefox')
                .where(appUpdateChannel='release')
                .where(appVersion=lambda x: x >= "52." and x < "53.")
                .records(sc))
release52_pings = get_pings_properties(release52_pings, properties_to_gather)





ds = Dataset.from_source('telemetry').where(docType='OTHER')
rec = ds.records(sc)
tls = rec.filter(lambda x: x.get("meta")["docType"] == "tls-13-study-v4")
         





beta53_pings = (Dataset.from_source('telemetry-sample')
                .where(docType='main')
                .where(appName='Firefox')
                .where(appUpdateChannel='beta')
                .where(appVersion=lambda x: x >= "53." and x < "54.")
                .where(submissionDate=lambda x: x >= '20170323' and x <= '2017331')
                .records(sc))
beta53_exp_pings = get_pings_properties(beta53_pings.filter(in_experiment), properties_to_gather)
res = utils.sum_histogram_experiment(sc, d, 100, "SSL_HANDSHAKE_VERSION", tls.predict_arm)      

