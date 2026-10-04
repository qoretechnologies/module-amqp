#!/usr/bin/python3
# Copyright (C) 2026 Qore Technologies, s.r.o.
# SPDX-License-Identifier: MIT
"""Check the public AMQP API and links in generated module documentation."""
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET

BUILD = Path(sys.argv.pop(1)).resolve()
SOURCE = Path(__file__).resolve().parents[1]


class Page(HTMLParser):
    def __init__(self, path):
        super().__init__()
        self.links = []
        self.text = []
        self.ids = set()
        self.images = []
        self.feed(path.read_text())

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'a' and 'href' in attrs:
            self.links.append(attrs['href'])
        for attr in ('id', 'name'):
            if attr in attrs:
                self.ids.add(attrs[attr])
        if tag == 'img':
            self.images.append(attrs.get('src'))

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
                        if url.fragment:
                            self.assertIn(unquote(url.fragment), Page(destination).ids, link)
                        destinations.add(url.path.split('/')[2])
        self.assertTrue({'amqp', 'AmqpUtil', 'AmqpDataProvider'} <= destinations, destinations)

    def test_companion_mainpages_are_indexes(self):
        for module, guides in {
            'AmqpDataProvider': ('configuration', 'cookbook', 'releasenotes'),
            'AmqpUtil': ('cookbook', 'releasenotes'),
        }.items():
            with self.subTest(module=module):
                folder = BUILD / 'docs' / module / 'html'
                index = Page(folder / 'index.html')
                self.assertIn(module.lower() + 'intro', index.ids)
                self.assertNotIn('class="fragment"', (folder / 'index.html').read_text())
                for guide in guides:
                    name = module.lower() + guide + '.html'
                    self.assertIn(name, index.links)
                    self.assertTrue((folder / name).is_file())
                release = Page(folder / (module.lower() + 'releasenotes.html'))
                self.assertEqual(1, ''.join(release.text).count('initial release'))
                self.assertNotIn('initial release', ''.join(index.text))

    def test_logos_and_legacy_bookmarks_are_preserved(self):
        for module in ('amqp', 'AmqpDataProvider'):
            folder = BUILD / 'docs' / module / 'html'
            self.assertIn('amqp-logo.svg', Page(folder / 'index.html').images)
            self.assertEqual((SOURCE / 'qlib/AmqpDataProvider/amqp-logo.svg').read_bytes(),
                             (folder / 'amqp-logo.svg').read_bytes())
        for module, anchors in {
            'AmqpDataProvider': ('amqpdataprovider_factory', 'amqpdataprovider_factory_options',
                                 'amqpdataprovider_examples', 'amqpdataprovider_connections',
                                 'amqpdataprovider_relnotes'),
            'AmqpUtil': ('amqputil_client_usage', 'amqputil_simple', 'amqputil_requestreply',
                         'amqputil_transactions', 'amqputilrelnotes'),
        }.items():
            self.assertTrue(set(anchors) <= Page(BUILD / 'docs' / module / 'html/index.html').ids)

    def test_shared_configuration_and_two_phase_tags(self):
        for name in ('Doxyfile', 'doxygen/Doxyfile.AmqpUtil', 'doxygen/Doxyfile.AmqpDataProvider'):
            for suffix in ('', '.final'):
                config = (BUILD / (name + suffix)).read_text()
                for setting in ('EXTERNAL_PAGES', 'EXTERNAL_GROUPS', 'MARKDOWN_SUPPORT'):
                    self.assertRegex(config, rf'(?m)^{setting}\s*=\s*NO\s*$')
                self.assertIn('header_template.html', config)
                self.assertIn('Qore-Q.ico', config)
                self.assertIn('DataProvider.tag=', config)
                self.assertIn('ConnectionProvider.tag=', config)
                if suffix:
                    self.assertRegex(config, r'GENERATE_TAGFILE\s*=\s*$')

    def test_cookbook_examples_parse_against_local_modules(self):
        env = dict(os.environ, QORE_MODULE_DIR=f'{SOURCE / "qlib"}:{BUILD}')
        env.pop('LD_PRELOAD', None)
        for module, filename, count in (
            ('AmqpDataProvider', 'AmqpDataProvider.qm', 2),
            ('AmqpUtil', 'AmqpUtil.qm', 6),
            ('AmqpUtil', 'AmqpConnectionObservable.qc', 1),
        ):
            source = (SOURCE / 'qlib' / module / filename).read_text()
            examples = re.findall(r'@code\{\.py\}\n(.*?)\s*@endcode', source, re.S)
            self.assertEqual(count, len(examples))
            for index, example in enumerate(examples):
                with self.subTest(source=filename, example=index):
                    # Type-check complete broker-dependent examples without opening a connection.
                    with tempfile.TemporaryDirectory(prefix='amqp-doc-example-') as directory:
                        path = Path(directory) / 'example.qr'
                        path.write_text(example)
                        result = subprocess.run(['qore', '--enable-debug', '--diag-format=json', str(path)], env=env,
                                                text=True, capture_output=True)
                    self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                    self.assertEqual('', result.stderr)
                    self.assertEqual([], json.loads(result.stdout), result.stdout)

    def test_cookbook_observer_delivers_the_event(self):
        source = (SOURCE / 'qlib/AmqpDataProvider/AmqpDataProvider.qm').read_text()
        example = re.findall(r'@code\{\.py\}\n(.*?)\s*@endcode', source, re.S)[1]
        # Exercise the actual documented observer without needing a broker. Keep the global queue
        # name from the example to catch accidental shadowing of its member in the callback.
        script = example.split('AmqpClient client(', 1)[0] + '''
Queue<hash<auto>> received();
MessageObserver observer(received);
observer.update(EVENT_AMQP_MESSAGE, {"body": {"order_id": 123}});
hash<auto> event = received.get(-1);
@assert(event.body.order_id == 123);
'''
        env = dict(os.environ, QORE_MODULE_DIR=f'{SOURCE / "qlib"}:{BUILD}')
        env.pop('LD_PRELOAD', None)
        with tempfile.TemporaryDirectory(prefix='amqp-doc-observer-') as directory:
            path = Path(directory) / 'observer.qr'
            path.write_text(script)
            result = subprocess.run(['qore', '-b', '--enable-debug', str(path)], env=env,
                                    text=True, capture_output=True, timeout=30)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual('', result.stderr)


if __name__ == '__main__':
    unittest.main()
