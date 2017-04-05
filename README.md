A pile of generic subroutines for using in Spark notebooks.

To use, do something like:

```
!rm -rf telemetry_utils && git clone https://github.com/ekr/telemetry_utils
sc.addPyFile("telemetry_utils/lib/utils.py")
import utils
sc.addPyFile("telemetry_utils/lib/tls.py")
import tls
```

This consists of two main modules:

* utils -- stuff which is supposed to be generic
* tls -- stuff which is supposed to be TLS-specific

The factoring is not really that clean. I also don't guarantee that
these implementations are really the right ones.

There is also a top-level file called boilerplate.py that I use to
store working state for whatever I'm doing now.


What's available is roughly:

# Utils








