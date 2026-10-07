# Copyright (C) 2026 Qore Technologies, s.r.o.
# SPDX-License-Identifier: MIT
# Use the pinned source epoch for RPM headers and installed file timestamps.
%global source_date_epoch_from_changelog 1
%global use_source_date_epoch_as_buildtime 1
%if v"%{rpmversion}" >= v"4.20"
%global build_mtime_policy clamp_to_source_date_epoch
%else
%global clamp_mtime_to_source_date_epoch 1
%endif
%bcond_without tests
%bcond_without docs
Name: qore-amqp-module
Version: 1.0.0
Release: 3%{?dist}
Summary: AMQP messaging and data providers for Qore
License: MIT
URL: https://github.com/qoretechnologies/module-amqp
Source0: %{name}-%{version}.tar.xz
%global _find_debuginfo_dwz_opts %{nil}
BuildRequires: cmake >= 3.21
BuildRequires: make
BuildRequires: gcc-c++
BuildRequires: pkgconfig(libqpid-proton-cpp)
%if %{with tests}
BuildRequires: qore-misc-tools >= 3.0.0~
%endif
BuildRequires: python3
BuildRequires: qore-devel >= 3.0.0~
BuildRequires: qore-rpm-macros >= 3.0.0~
BuildRequires: qore-xml-module >= 1.0
# XML is captured by the AOT helpers and must also be present at runtime.
Requires: qore-xml-module%{?_isa} >= 1.0
%if %{with docs}
BuildRequires: doxygen
BuildRequires: qore-devel(module-doc-peers) = 1
%if 0%{?suse_version}
BuildRequires: util-linux
%else
BuildRequires: util-linux-core
%endif
%endif
%{?qore_enable_aot_post}

%description
AMQP 1.0 messaging through Apache Qpid Proton, compiled AmqpUtil helpers and
AmqpDataProvider integration, source modules, SDK metadata and translations.
Network messaging requires an AMQP 1.0 broker.

%if %{with docs}
%package doc
Summary: AMQP module reference documentation and examples
BuildArch: noarch
%description doc
API references and examples for Qore's native messaging and helper modules.
%endif

%prep
%autosetup
%build
%{?set_build_flags}
. %{_rpmconfigdir}/qore/module-env.sh
qore_set_source_prefix_maps "%{qore_debug_source_dir}"
cmake -S . -B build -G 'Unix Makefiles' \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS_RELEASE=-DNDEBUG \
  -DCMAKE_INSTALL_PREFIX=%{_prefix} \
  -DCMAKE_SKIP_RPATH=ON -DCMAKE_IGNORE_PREFIX_PATH=/usr/local \
  -DQore_DIR=%{_libdir}/cmake/Qore -DQORE_EXECUTABLE=/usr/bin/qore \
  -DQORE_QPP_EXECUTABLE=/usr/bin/qpp -DQORE_QCC_EXECUTABLE=/usr/bin/qcc \
  -DQORE_BUILD_AOT_MODULES=ON -DQORE_AOT_LINK_SOURCE_MODULES=OFF \
  -DQORE_GENERATE_JAVA_BINDINGS=OFF \
  -DQORE_MODULE_DIR_FOR_DOCS:STRING="$QORE_MODULE_DIR:$PWD/qlib" \
  -DQORE_QM_METADATA_ENV:STRING="QORE_MODULE_DIR=$QORE_MODULE_DIR:$PWD/qlib;QORE_MODULE_DIR_ONLY=1;QORE_INCLUDE_DIR=;LD_LIBRARY_PATH=" \
  -DCMAKE_DISABLE_FIND_PACKAGE_Doxygen=%{!?with_docs:ON}%{?with_docs:OFF}
cmake --build build -- %{?_smp_mflags}
%if %{with docs}
for doxyfile in build/Doxyfile.final build/doxygen/Doxyfile.*; do
  printf "\nWARN_AS_ERROR = FAIL_ON_WARNINGS\n" >> "$doxyfile"
done
cmake --build build --target docs -- %{?_smp_mflags}
%endif
%install
DESTDIR=%{buildroot} cmake --install build
%qore_install_aot_sources qlib
find %{buildroot}%{_libdir}/qore-modules -type f -name '*.qmod' -exec chmod 755 {} +
%if %{with docs}
install -d %{buildroot}%{_docdir}/%{name}-doc
cp -a build/docs %{buildroot}%{_docdir}/%{name}-doc/
install -d %{buildroot}%{_docdir}/%{name}-doc/examples/test
install -m644 test/*.qtest %{buildroot}%{_docdir}/%{name}-doc/examples/test/
hardlink -t -O %{buildroot}%{_docdir}/%{name}-doc
%endif
%check
%if %{with tests}
. %{_rpmconfigdir}/qore/module-env.sh
unset AMQP_TEST_URL AMQP_TLS_TEST_URL AMQP_TLS_MTLS_URL AMQP_TLS_CA AMQP_TLS_WRONG_CA AMQP_TLS_CLIENT_CERT AMQP_TLS_CLIENT_KEY
for test in test/*.qtest; do
  timeout 600 /usr/bin/qore -b --enable-debug \
    -l "$PWD/build/amqp-api-$(/usr/bin/qore --latest-module-api).qmod" \
    -l "$PWD/build/qlib-qmod/AmqpUtil/AmqpUtil.qmod" \
    -l "$PWD/build/qlib-qmod/AmqpDataProvider/AmqpDataProvider.qmod" "$test" -v
done
qore-data-provider-i18n --no-color --check-source-tree --require-standard-locales \
  --require-complete-locales --output "$PWD/qlib"
%if %{with docs}
python3 -B -W error test/test_docs.py build -v
%endif
%endif
%files
%license COPYING.MIT
%doc README.md
%{_libdir}/qore-modules/amqp-api-*.qmod
%{_libdir}/qore-modules/AmqpUtil/
%{_datadir}/qore-modules/AmqpUtil/
%{_libdir}/qore-modules/AmqpDataProvider/
%{_datadir}/qore-modules/AmqpDataProvider/
%dir %{_datadir}/qore/metadata/amqp
%{_datadir}/qore/metadata/amqp/*.meta.json
%{_datadir}/qore/i18n/
%if %{with docs}
%files doc
%license COPYING.MIT
%doc %{_docdir}/%{name}-doc/
%endif
%changelog
* Tue Oct 06 2026 David Nichols <david@qore.org> - 1.0.0-3
- Require XML at runtime for the compiled AmqpUtil and provider helpers.
- Require the documentation SDK peer-index capability for documentation builds.

* Tue Oct 06 2026 David Nichols <david@qore.org> - 1.0.0-2
- Use the documented XML build dependency and omit unshipped Java bindings.
- Remove the unused installation-directory override and qualify current documentation.
* Thu Oct 01 2026 David Nichols <david@qore.org> - 1.0.0-1
- Package native messaging, compiled helpers, provider translations and offline tests.
