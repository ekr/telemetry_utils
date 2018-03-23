
# Include utils.py
# Include tls.py

histograms_to_study = ["SSL_HANDSHAKE_RESULT", "SSL_HANDSHAKE_VERSION", "HTTP_CHANNEL_DISPOSITION"]

a = run_analysis(sc, 60, "beta", True, "20180310", "20180324", lambda x: True, lambda x: predict_arm(x, "tls13-rollout-bug1442042@mozilla.org", .05), ["SSL_HANDSHAKE_RESULT", "SSL_HANDSHAKE_VERSION", "HTTP_CHANNEL_DISPOSITION"], None)
