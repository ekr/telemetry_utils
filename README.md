A pile of generic subroutines for using in Spark notebooks.

To use, do something like:

```
!rm -rf telemetry_utils && git clone https://github.com/ekr/telemetry_utils
sc.addPyFile("telemetry_utils/lib/utils.py")
import utils
sc.addPyFile("telemetry_utils/lib/tls.py")
import tls
```

This consists of two main modules

* utils -- stuff which is supposed to be generic
* tls -- stuff which is supposed to be TLS-specific

The factoring is not really that clean. I also don't guarantee that
these implementations are really the right ones.

There is also a top-level file called boilerplate.py that I use to
store working state for whatever I'm doing now.


More detail about what's currently here:

# Utils

Exports a pile of functions primarily for building contingency tables
of telemetry pings for categorical histograms. Supports both raw
contingency tables and contingency tables normalized so that each
clientId is given equal weight. Example usage:

~~~~
> y = run_comparison_panel_by_client(sc, beta53_exp_pings_full, histograms, predict_arm, tls.HISTOGRAM_LABELS)

> y["SSL_HANDSHAKE_RESULT"][-10:]

[['SSL_ERROR_RX_RECORD_TOO_LONG',
  [113.64288622716715, 0.00029950633371592192],
  [119.38245840980389, 0.00031490023636762703]],
 ['UNKNOWN_ERROR',
  [122.55444671533461, 0.00032299279114505965],
  [127.05272981906997, 0.00033513244059557568]],
 ['SEC_ERROR_EXPIRED_CERTIFICATE',
  [129.50711975353667, 0.00034131659195943599],
  [127.34166381850159, 0.00033589457421158263]],
 ['PR_CONNECT_ABORTED_ERROR',
  [334.64666271796131, 0.00088196277275616106],
  [370.08002260109578, 0.00097617596541680436]],
 ['SSL_ERROR_BAD_CERT_DOMAIN',
  [389.73953937313161, 0.0010271602950002675],
  [391.14682819684884, 0.0010317447830637083]],
 ['PR_WOULD_BLOCK_ERROR',
  [1510.9999299765564, 0.0039822470573974813],
  [1534.3472646496039, 0.0040472136588913113]],
 ['SEC_ERROR_UNKNOWN_ISSUER',
  [1932.3508136285761, 0.005092719191291702],
  [1901.8226295478398, 0.0050165192068513751]],
 ['PR_END_OF_FILE_ERROR',
  [2747.764018312364, 0.0072417443305353843],
  [3189.4835792917138, 0.0084130377811615347]],
 ['PR_CONNECT_RESET_ERROR',
  [4798.9236986272726, 0.012647584820093274],
  [4764.6313987429176, 0.012567872815270723]],
 ['SUCCESS',
  [366785.06962116639, 0.96666368754820675],
  [365977.09995070362, 0.96535351017826765]]]
~~~~
                                        

# tls

A pile of stuff you won't want that lets you process TLS experiments,
but also ```tls.HISTOGRAM_LABELS``` contains a map table for bucket names
for the following histograms.

* SSL_HANDSHAKE_VERSION
* SSL_HANDSHAKE_RESULT
* HTTP_CHANNEL_DISPOSITION













