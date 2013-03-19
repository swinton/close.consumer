#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A specific implementation, using redis_ for persistence.

  .. _redis: http://code.google.com/p/redis/
"""
import sys
import logging
import json

from base import BaseConsumer, BaseManager, BaseWSGIApp


class Consumer(BaseConsumer):
    """Gets data delimited_ by length.

      .. _delimited: http://apiwiki.twitter.com/Streaming-API-Documentation#delimited
    """

    def get_data(self):
        line = self._readline_chunked()
        if line.isdigit():
            return self._read_chunked(int(line))


class Manager(BaseManager):
    """Generate the filter predicates and handle the data.
    """

    def get_headers(self):
        return {}

    def get_params(self):
        """Hard-coding params for now.
        """
        return dict(track="justinbieber")

    def handle_data(self, data):
        """Append the data to a redis list and notify that
          we've done so.
        """
        item = json.loads(data)
        print item["text"]


class WSGIApp(BaseWSGIApp):
    def handle_request_params(self, action, params):
        """Save the predicates.
        """

        logging.warning(
            '@@ no validation is being applied to follow and track params'
        )

        track = params.get('track', None)
        print "track", track


def parse_options():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option(
        '--logging',
        dest='log_level',
        action='store',
        type='string',
        default='info'
    )
    parser.add_option(
        '--host',
        dest='host',
        action='store',
        type='string',
        help='the host you want the streaming API ``Consumer`` to connect to',
        default='stream.twitter.com'
    )
    parser.add_option(
        '--path',
        dest='path',
        action='store',
        type='string',
        help='the path you want the streaming API ``Consumer.conn`` to request',
        default='/1/statuses/filter.json?delimited=length'
    )
    parser.add_option(
        '--username',
        dest='username',
        action='store',
        type='string',
        help='the basic http auth username you want to use, if any',
        default=''
    )
    parser.add_option(
        '--password',
        dest='password',
        action='store',
        type='string',
        help='the basic http auth password you want to use, if any',
        default=''
    )
    parser.add_option(
        '--port',
        dest='port',
        action='store',
        type='int',
        help='the local port you want to expose the ``WSGIApp`` on',
        default=8282
    )
    parser.add_option(
        '--serve-and-start',
        dest='should_start_consumer',
        action='store_true',
        help='start a consumer by default',
        default=True
    )
    parser.add_option(
        '--serve-only',
        dest='should_start_consumer',
        action='store_false',
        help='don\'t start a consumer by default'
    )
    return parser.parse_args()[0]


def main():
    from gevent import wsgi

    options = parse_options()
    logging.basicConfig(stream=sys.stderr,
                        format='%(asctime)s %(levelname)s %(module)s %(lineno)d %(message)s',
                        level=getattr(logging, options.log_level.upper()))

    kwargs = {}
    if options.username:
        kwargs['username'] = options.username
    if options.password:
        kwargs['password'] = options.password

    manager = Manager(Consumer, options.host, options.path, **kwargs)
    if options.should_start_consumer:
        manager.start_a_consumer()

    app = WSGIApp(manager=manager)
    server = wsgi.WSGIServer(('', options.port), app.handle_requests)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
