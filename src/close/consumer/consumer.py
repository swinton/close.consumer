#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A specific implementation, using redis_ for persistence.

  .. _redis: http://code.google.com/p/redis/
"""
import sys
import logging
import json

import gevent

from termcolor import colored

from base import BaseConsumer, BaseManager, BaseWSGIApp, notification_queue

from utils import generate_auth_header, generate_oauth_header

import settings


class Consumer(BaseConsumer):
    """Gets data delimited_ by length.

      .. _delimited: http://apiwiki.twitter.com/Streaming-API-Documentation#delimited
    """

    def _notify(self, event_name, data):
        """Puts an {event_name: data} item into a gevent queue_

          .. _queue: http://www.gevent.org/gevent.queue.html
        """

        item = {}
        if event_name == "data":
            data = (self, data)  # Send self as well, so manager knows which consumer sent this event
        item[event_name] = data
        notification_queue.put_nowait(item)

    def get_data(self):
        line = self._readline_chunked()
        if line.isdigit():
            return self._read_chunked(int(line))


class Manager(BaseManager):
    """Generate the filter predicates and handle the data.
    """

    colors = {}

    def get_headers(self):
        return {}

    def get_params(self):
        """Read params from settings.PARAMS_LIST.
        """
        # return dict(settings.PARAMS_LIST.pop())
        return {}

    def handle_data(self, data):
        """Just print the data.
        """
        # Unpack data
        consumer, data = data
        try:
            item = json.loads(data)
            print colored(repr(consumer.params), self.colors[consumer.id]), item["text"]
        except Exception as e:
            logging.error(e)
            logging.error(repr(data))

    def _handle_connect(self, consumer_id):
        """In our implementation, we want to allow concurrent consumers to run.
        """
        pass

    def start_a_consumer(self):
        """Fire up a new Consumer.
        """

        # try:
        #     username, password = settings.ACCOUNTS_LIST.pop()
        # except:
        #     username, password = (self.username, self.password)

        # logging.info('Starting a consumer for {0}'.format(username))
        oauth_options = settings.ACCOUNTS_LIST.pop()
        oauth_options["url"] = "".join(("https://", self.host, self.path))
        oauth_options["method"] = "POST"
        oauth_options["parameters"] = self.params

        # create the new consumer
        consumer = self.consumer_class(
            path=self.path,
            host=self.host,
            params=self.get_params(),
            headers=self.get_headers(),
            auth_method=generate_oauth_header,
            auth_options=oauth_options,
        )
        logging.info(consumer.id)

        # start the consumer in a new greenlet
        g = gevent.spawn(consumer.run)

        # put it in self.consumers
        self.consumers[consumer.id] = g
        self.colors[consumer.id] = settings.COLORS.pop()
        logging.info(self.consumers)

        return consumer


class WSGIApp(BaseWSGIApp):
    pass


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
        default='userstream.twitter.com'
    )
    parser.add_option(
        '--path',
        dest='path',
        action='store',
        type='string',
        help='the path you want the streaming API ``Consumer.conn`` to request',
        default='/2/user.json'
    )
    parser.add_option(
        '--params',
        dest='params',
        action='callback',
        callback=json.loads,
        help='the parameters you want to use with the streaming API request',
        default={"delimited": "length"}
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

    manager = Manager(Consumer, options.host, options.path, options.params, **kwargs)
    if options.should_start_consumer:
        manager.start_a_consumer()

    app = WSGIApp(manager=manager)
    http_server = wsgi.WSGIServer(('', options.port), app.handle_requests)
    # from flaskapp import app
    # http_server = wsgi.WSGIServer(('', options.port), app)

    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
