#!/usr/bin/env python

import sys
import logging

from flask import Flask

from consumer import Consumer, Manager, parse_options


class App(Flask):
    _manager = None

    @property
    def manager(self):
        assert self._manager is not None
        return self._manager

    @manager.setter
    def manager(self, mgr):
        assert type(mgr) is Manager
        self._manager = mgr
app = App(__name__)


@app.route("/")
def hello():
    return "Hello World!\n"


@app.route("/start")
def start():
    app.manager.start_a_consumer()
    return "OK\n"


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

    # Create a Manager instance and store it on the app
    app.manager = Manager(Consumer, options.host, options.path, **kwargs)
    http_server = wsgi.WSGIServer(('', options.port), app)

    try:
        logging.info("Serving on port {0}".format(options.port))
        http_server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
