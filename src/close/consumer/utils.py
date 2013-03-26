#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Helper functions.
"""

import base64
import hashlib
import random
import time
import urllib

from . import oauth


def unicode_urlencode(params):
    if isinstance(params, dict):
        params = params.items()
    return urllib.urlencode([(
                k,
                isinstance(v, unicode) and v.encode('utf-8') or v
            ) for k, v in params
        ]
    )


def generate_hash(algorithm='sha1', s=None):
    """Generates a random string.
    """

    # if a string has been provided use it, otherwise default
    # to producing a random string
    s = s is None and '%s%s' % (random.random(), time.time()) or s
    hasher = getattr(hashlib, algorithm)
    return hasher(s).hexdigest()


def generate_auth_header(username, password):
    auth = base64.b64encode(u'%s:%s' % (username, password))
    return 'Basic %s' % auth


def generate_oauth_header(consumer_key, consumer_secret, access_key, access_secret, url, method="POST", parameters=None):
    consumer = oauth.OAuthConsumer(consumer_key, consumer_secret)
    token = oauth.OAuthToken(access_key, access_secret)
    sigmethod = oauth.OAuthSignatureMethod_HMAC_SHA1()
    request = oauth.OAuthRequest.from_consumer_and_token(
        consumer, http_url=url, http_method=method,
        token=token, parameters=parameters
    )
    request.sign_request(sigmethod, consumer, token)
    header = request.to_header()
    return header["Authorization"]
