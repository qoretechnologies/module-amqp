#!/usr/bin/python3
# Copyright (C) 2026 Qore Technologies, s.r.o.
# SPDX-License-Identifier: MIT
"""Exercise manifest-based uninstallation without touching the host prefix."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

TEMPLATE = Path(__file__).resolve().parents[1] / 'cmake/cmake_uninstall.cmake.in'


class UninstallTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='amqp-uninstall-', dir=Path.cwd())
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.script = self.root / 'uninstall.cmake'
        self.script.write_text(TEMPLATE.read_text().replace('@CMAKE_CURRENT_BINARY_DIR@', str(self.root)))
        self.manifest = self.root / 'install_manifest.txt'

    def run_uninstall(self, destdir=None, error=None):
        env = dict(os.environ)
        env.pop('DESTDIR', None)
        if destdir is not None:
            env['DESTDIR'] = str(destdir)
        result = subprocess.run(['cmake', '-P', str(self.script)], env=env, capture_output=True, text=True)
        if error is None:
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertEqual('', result.stderr)
        else:
            self.assertNotEqual(0, result.returncode)
            self.assertIn(error, result.stderr)

    def test_missing_manifest(self):
        self.run_uninstall(error='Cannot find install manifest')

    def test_spaces_missing_files_and_repeated_uninstall(self):
        target = self.root / 'installed file with spaces'
        target.write_text('installed content')
        unrelated = self.root / 'unrelated'
        unrelated.write_text('keep')
        self.manifest.write_text(str(target) + '\n' + str(self.root / 'already absent') + '\n')
        self.run_uninstall()
        self.assertFalse(target.exists())
        self.assertEqual('keep', unrelated.read_text())
        self.run_uninstall()

    def test_destdir_keeps_host_file(self):
        target = self.root / 'host prefix' / 'module.qmod'
        target.parent.mkdir()
        target.write_text('host content')
        stage = self.root / 'staging root'
        staged = stage / str(target).lstrip('/')
        staged.parent.mkdir(parents=True)
        staged.write_text('staged content')
        self.manifest.write_text(str(target) + '\n')
        self.run_uninstall(stage)
        self.assertFalse(staged.exists())
        self.assertEqual('host content', target.read_text())

    def test_symlinks_preserve_targets(self):
        target = self.root / 'actual directory'
        target.mkdir()
        content = target / 'keep'
        content.write_text('keep')
        link = self.root / 'directory link'
        link.symlink_to(target)
        dangling = self.root / 'dangling link'
        dangling.symlink_to(self.root / 'missing')
        self.manifest.write_text(str(link) + '\n' + str(dangling) + '\n')
        self.run_uninstall()
        self.assertFalse(link.is_symlink())
        self.assertFalse(dangling.is_symlink())
        self.assertEqual('keep', content.read_text())

    def test_directory_is_rejected(self):
        target = self.root / 'directory'
        target.mkdir()
        self.manifest.write_text(str(target) + '\n')
        self.run_uninstall(error='Refusing to remove a directory')
        self.assertTrue(target.is_dir())

    def test_relative_manifest_entry_is_rejected(self):
        self.manifest.write_text('relative-file\n')
        self.run_uninstall(error='Install manifest contains a relative path')


if __name__ == '__main__':
    unittest.main()
