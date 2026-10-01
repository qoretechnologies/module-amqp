#!/usr/bin/python3
# Copyright (C) 2026 Qore Technologies, s.r.o.
# SPDX-License-Identifier: MIT
"""Check the public AMQP API and links in generated module documentation."""
from html.parser import HTMLParser
from pathlib import Path
import sys
import unittest
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET

BUILD = Path(sys.argv.pop(1)).resolve()


class Page(HTMLParser):
    def __init__(self, path):
        super().__init__()
        self.links = []
        self.text = []
        self.feed(path.read_text())

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'a' and 'href' in attrs:
            self.links.append(attrs['href'])

    def handle_data(self, data):
        self.text.append(data)


class DocsTest(unittest.TestCase):
    def test_public_native_classes_and_methods_have_pages(self):
        compounds = {c.findtext('name'): c for c in ET.parse(BUILD / 'amqp.tag').findall('compound')}
        for name, method in [('AmqpConnection', 'connect'), ('AmqpMessage', 'getBody')]:
            with self.subTest(name=name):
                compound = compounds['Qore::Amqp::' + name]
                self.assertIn(method, [m.findtext('name') for m in compound.findall('member')])
                self.assertTrue((BUILD / 'docs/amqp/html' / compound.findtext('filename')).is_file())

    def test_cross_module_links_resolve_to_installed_sibling_pages(self):
        destinations = set()
        for module in ('amqp', 'AmqpUtil', 'AmqpDataProvider'):
            for path in (BUILD / 'docs' / module / 'html').glob('*.html'):
                for link in Page(path).links:
                    url = urlsplit(link)
                    if not url.scheme and url.path.startswith('../../'):
                        destination = path.parent / unquote(url.path)
                        self.assertTrue(destination.is_file(), f'{path.name}: {link}')
                        destinations.add(url.path.split('/')[2])
        self.assertTrue({'amqp', 'AmqpUtil', 'AmqpDataProvider'} <= destinations, destinations)



if __name__ == '__main__':
    unittest.main()
