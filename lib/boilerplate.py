
# Include utils.py
# Include tls.py

histograms_to_study = ["SSL_HANDSHAKE_RESULT", "SSL_HANDSHAKE_VERSION", "HTTP_CHANNEL_DISPOSITION"]

def running_current_add_on(x):
    try:
        if not "clientId" in x:
            return False
        return x["environment"]["addons"]["activeAddons"]["tls13-version-fallback-rollout-bug1448176@mozilla.org"]["version"] == "1.0"
    except:
        return False
a = run_analysis(sc, 60, "beta", True, "20180401", "20180415", running_current_add_on, lambda x: predict_arm(x, "tls13-version-fallback-rollout-bug1448176@mozilla.org", .10), ["SSL_HANDSHAKE_RESULT", "SSL_HANDSHAKE_VERSION", "HTTP_CHANNEL_DISPOSITION"], HISTOGRAM_LABELS)



def running_current_add_on(x):
    try:
        if not "clientId" in x:
            return False
        return x["environment"]["addons"]["activeAddons"]["tls13-rollout-bug1442042@mozilla.org"]["version"] == "8.0"
    except:
        return False
a = run_analysis(sc, 59, "release", True, "20180401", "20180430", running_current_add_on, lambda x: predict_arm(x, "tls13-rollout-bug1442042@mozilla.org", .95), ["SSL_HANDSHAKE_RESULT", "SSL_HANDSHAKE_VERSION", "HTTP_CHANNEL_DISPOSITION"], HISTOGRAM_LABELS)
