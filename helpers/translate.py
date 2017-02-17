import re
import sys
import json

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

for e in ERRORS:
    print "%d: \"%s\","%(e,ERRORS[e])
