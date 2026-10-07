RPM packaging
=============

Copyright 2026 Qore Technologies, s.r.o.

qore-amqp-module.spec targets Fedora, Enterprise Linux and openSUSE using the
Qore 3.0 SDK and qore-rpm-macros. Apache Qpid Proton C++ is a system dependency.
Native and AOT modules, source fallbacks, SDK metadata, provider resources and
translations are installed together; documentation is a separate package.
Qore ABI and minimum runtime requirements are generated from the built modules.
XML is loaded while compiling the AOT helpers, so the RPM declares a native
qore-xml-module runtime dependency as well as its build dependency. The source
module keeps its optional XML loading behavior.

From qore-packaging, prepare the committed source and build offline::

    python3 tools/packaging.py prepare --repo ../module-amqp --ref COMMIT \
      --name qore-amqp-module --version 1.0.0 --spec qore-amqp-module.spec \
      --output work/amqp-source
    python3 tools/build-local.py --source work/amqp-source \
      --image TARGET_SDK_IMAGE --output results/amqp-build --jobs 2

The default build runs all local suites against the newly compiled native and
AOT modules, checks translation completeness and builds the three HTML references
with strict final-pass Doxygen checks. The initial index passes break the native,
helper and provider documentation cycle; final passes resolve all references.
The HTML regression verifies public methods and every sibling-module link.
--without docs and --without tests are diagnostic options, not qualification.

Broker integration and TLS/mTLS suites require the endpoints and certificates
documented in test/docker_test and test/amqp-tls.qtest. The offline build clears
those variables and the suites report their unavailable-broker cases. Connected
qualification must run them against a local broker before publishing a target.
An installed runtime check must preload the installed AOT helpers and run copied
tests outside the checkout without a Qore SDK or development module paths.

Run the installed offline check from the checkout with a fresh directory::

    QORE_RPM_TEST_TMP=/tmp/amqp-installed rpm/tests-installed-runtime

It copies only tests, clears development paths and broker variables, and explicitly
loads the installed AOT helpers. The package must work without qore-devel or a C++
compiler; broker/TLS checks remain a separate connected qualification step.

RPM lint reports no errors. The expected policy warnings are the locally prepared
Source0 archive and Qore's hidden catalog ownership directory. That manifest is
needed to reconcile translations on installation; it is part of the SDK's
catalog contract and must remain in the runtime package.
