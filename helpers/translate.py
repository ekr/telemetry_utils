import re
import sys

BASE = "/Users/ekr/dev/gecko/gecko-dev/"
ERRORS = {}

def read_ssl_errors(file):
    f = open(file)
    for l in f:
        m = re.search("(SSL_\S+).*=\s*.*SSL_ERROR_BASE.*\+\s*(\d+)", l)
        if m is not None:
            ERRORS[int(m.group(2))] = m.group(1)


def read_sec_errors(file):
    f = open(file)
    for l in f:
        m = re.search("(SEC_\S+).*=\s*.*SEC_ERROR_BASE.*\+\s*(\d+)", l)
        if m is not None:
            ERRORS[int(m.group(2)) + 256] = m.group(1)

def read_nspr_errors(file):
    f = open(file)
    for l in f:
        m = re.search("\#define (PR_\S+).*-(\d+)L", l)
        if m is not None:
            ERRORS[6000 - int(m.group(2)) + 512] = m.group(1)

def read_pkix_errors(file):
    f = open(file)
    for l in f:
        m = re.search("(MOZILLA_PKIX_ERROR_\S+).*=\s*.*ERROR_BASE.*\+\s*(\d+)", l)
        if m is not None:
            ERRORS[int(m.group(2)) + 640] = m.group(1)
    

read_ssl_errors("%s/security/nss/lib/ssl/sslerr.h"%BASE)
read_sec_errors("%s/security/nss/lib/util/secerr.h"%BASE)
read_nspr_errors("%s/nsprpub/pr/include/prerr.h"%BASE)
read_pkix_errors("%s/security/pkix/include/pkix/pkixnss.h"%BASE)
ERRORS[0] = "SUCCESS"

COUNTS = {}
RESULTS= []
SUM = 0
CATEGORIES = {
    "SUCCESS":0,
    "FAIL":0,
    "ERROR":0,
    "UNKNOWN":0,
}

ERROR_RESULTS = [
    "PR_CONNECT_RESET_ERROR",
    "PR_END_OF_FILE_ERROR",
    "PR_CONNECT_ABORTED_ERROR"
]

FAIL_RESULTS = [
    "SSL_ERROR_BAD_CERT_DOMAIN",
    "SEC_ERROR_EXPIRED_CERTIFICATE",
    "SEC_ERROR_UNKNOWN_ISSUER",
    "SEC_ERROR_CERT_SIGNATURE_ALGORITHM_DISABLED",
    "SSL_ERROR_UNRECOGNIZED_NAME_ALERT",
]

def categorize(e):
    if e == "SUCCESS":
        return e
    if e in ERROR_RESULTS:
        return "ERROR"
    if e in FAIL_RESULTS:
        return "FAIL"
    return "UNKNOWN"

for l in sys.stdin:
    m = re.search("(\d+):\s*(\d+)", l)
    if m is not None:
        err = ERRORS[int(m.group(1))]
        RESULTS.append(err)
        COUNTS[err] = int(m.group(2))
        SUM += COUNTS[err]
        CATEGORIES[categorize(err)] += int(m.group(2))
        
RESULTS.sort(lambda a,b: cmp(COUNTS[a], COUNTS[b]))
for r in RESULTS:
    print r,COUNTS[r],float(COUNTS[r])/float(SUM),categorize(r)

print    
for c in CATEGORIES:
    print c, CATEGORIES[c],float(CATEGORIES[c])/float(SUM)    
