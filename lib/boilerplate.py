
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
a = run_analysis(sc, 60, "release", True, "20180401", "20180430", running_current_add_on, lambda x: predict_arm(x, "tls13-rollout-bug1442042@mozilla.org", .95), ["SSL_HANDSHAKE_RESULT", "SSL_HANDSHAKE_VERSION", "HTTP_CHANNEL_DISPOSITION"], HISTOGRAM_LABELS)


def running_experiment_add_on(x, experiment, version):
    try:
        if not "clientId" in x:
            return False
        return x["environment"]["addons"]["activeAddons"][experiment]["version"] == version
    except:
        return False
    
def run_experiment_analysis(experiment_name, experiment_version, frac, start, stop, version, channel):
    a = run_analysis(sc, version, channel, True, start, stop, lambda x: running_experiment_add_on(x, experiment_name, experiment_version), lambda x: predict_arm(x, experiment_name, frac), ["SSL_HANDSHAKE_RESULT", "SSL_HANDSHAKE_VERSION", "HTTP_CHANNEL_DISPOSITION"], HISTOGRAM_LABELS)
    return a

run_experiment_analysis("tls13-version-fallback-rollout-bug1462099@mozilla.org", "4.0", .20, "20180616", "20180622", 60, "release")



a = run_analysis(sc, 60, "beta", False, "20180613", "20180627", lambda x: "control", lambda x: True, ["SSL_HANDSHAKE_RESULT", "SSL_HANDSHAKE_VERSION", "HTTP_CHANNEL_DISPOSITION"], HISTOGRAM_LABELS)


def doing_rollout(x):
    try:
        if "rollout-release-61-tls-fallback-1-3" in x["environment/experiments"]:
            return "treatment"
        return "control"
    except:
        return "control"
            
a = run_analysis(sc, 61, "beta", True, "20180613", "20180627", lambda x: True, doing_rollout, ["SSL_HANDSHAKE_RESULT", "SSL_HANDSHAKE_VERSION", "HTTP_CHANNEL_DISPOSITION"], HISTOGRAM_LABELS)



# Compare statistics between two study arms
def run_analysis_shield(sc, ver, channel, sample, start, end, arm_func, histograms_to_study, trans):
    if sample:
        s = "telemetry-sample"
    else:
        s = "telemetry"
    ds = (Dataset.from_source(s)
          .where(docType='main')
          .where(appName='Firefox')
          .where(appUpdateChannel=channel)
          .where(appVersion=lambda x: x >= "%d."%ver and x < "%d."%(ver+1))
          .where(submissionDate=lambda x: x >= start and x <= end)
          .records(sc))
    in_exp_raw = ds
    in_exp_raw.cache()
    properties_to_gather = [payload(x) for x in histograms_to_study]
    properties_to_gather.append("clientId")
    properties_to_gather.append("environment/experiments")
    in_exp = get_pings_properties(in_exp_raw, properties_to_gather)
    in_exp.cache()
    shield = in_exp.filter(lambda x: x["environment/experiments"] != None)
    print("Total pings")
    print shield.count()
    control = shield.filter(lambda x: arm_func(x) == "control")
    control_results = run_comparison_panel_by_client(sc, control, histograms_to_study, lambda x: "control", trans)
    treatment = shield.filter(lambda x: arm_func(x) == "treatment")
    treatment_results = run_comparison_panel_by_client(sc, treatment, histograms_to_study, lambda x: "control", trans)
    control_count = control.count()
    treatment_count = treatment.count()
    print "Control=%d Treatment=%d"%(control_count, treatment_count)
    return [[in_exp, in_exp_raw, control, treatment], [control_results, treatment_results]]

def print_analysis_inner(inner):
    for h in inner:
        render_compared_histogram(inner[h])
    
def print_shield_analysis(a):
    print "Control"
    print_analysis_inner(a[1][0])
    print "Treatment"
    print_analysis_inner(a[1][1])
        
a = run_analysis(sc, 61, "release", True, "20180716", "20180731", lambda x: True, lambda x: "control", ["SSL_HANDSHAKE_RESULT", "SSL_HANDSHAKE_VERSION", "HTTP_CHANNEL_DISPOSITION"], HISTOGRAM_LABELS)
