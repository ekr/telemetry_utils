import ujson as json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import plotly.plotly as py
import IPython

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

properties_to_gather=[utils.payload("SSL_HANDSHAKE_VERSION"), utils.payload("SSL_TLS12_INTOLERANCE_REASON_PRE"), utils.payload("SSL_HANDSHAKE_RESULT"), utils.payload("SSL_TLS13_INTOLERANCE_REASON_PRE")]
nightly_pings = (Dataset.from_source('telemetry')
                .where(docType='main')
                .where(appName='Firefox')
                .where(appUpdateChannel='nightly')
                .where(appBuildId=lambda x: x >= "20170215000000")
                .records(sc, sample=1))
nightly_pings = get_pings_properties(nightly_pings, properties_to_gather)

x = utils.sum_histogram(sc, nightly_pings, 800, "SSL_HANDSHAKE_RESULT")
tls.translate_errors(x)

