# -*- mode: qore; indent-tabs-mode: nil -*-
#! @file AmqpDataProvider.qc Root data provider for AMQP messaging

/*  AmqpDataProvider.qc Copyright 2026 Qore Technologies, s.r.o.

    Permission is hereby granted, free of charge, to any person obtaining a
    copy of this software and associated documentation files (the "Software"),
    to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense,
    and/or sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
    DEALINGS IN THE SOFTWARE.
*/

#! Contains all public definitions in the AmqpDataProvider module
public namespace AmqpDataProvider {

#! The main app name for action catalog registration
public const AppName = "AMQP";

#! Root data provider for AMQP 1.0 messaging
/** Provides hierarchical access to AMQP addresses via the data provider API.

    The provider hierarchy is:
    - AmqpDataProvider (root -- connection)
      - \<address\> (AmqpAddressDataProvider -- supports doRequest for send)
        - events (AmqpEventDataProvider -- observable incoming messages)

    @par Example
    @code{.py}
AmqpDataProvider provider({
    "url": "amqp://guest:guest\@localhost:5672",
    "addresses": "queue1,queue2",
});
auto result = provider.getChildProvider("queue1").doRequest({"body": "Hello!"});
    @endcode

    @since AmqpDataProvider 1.0
*/
public class AmqpDataProvider inherits AbstractDataProvider {
    public {
        #! Provider info
        const ProviderInfo = <DataProviderInfo>{
            "type": "AmqpDataProvider",
            "supports_children": True,
            "constructor_options": ConstructorOptions,
            "children_can_support_apis": True,
            "children_can_support_observers": True,
        };

        #! Constructor options
        const ConstructorOptions = {
            "url": <DataProviderOptionInfo>{
                "display_name": "URL",
                "short_desc": "The AMQP broker URL",
                "type": AbstractDataProviderType::get(StringType),
                "desc": "The URL of the AMQP broker (e.g., "
                    "`amqp://guest:guest@localhost:5672` or `amqps://...`)",
                "required": True,
            },
            "addresses": <DataProviderOptionInfo>{
                "display_name": "Addresses",
                "short_desc": "Comma-separated list of AMQP addresses",
                "type": AbstractDataProviderType::get(StringType),
                "desc": "Comma-separated list of AMQP addresses (queues/topics) to expose "
                    "as child providers",
            },
            "use_management": <DataProviderOptionInfo>{
                "display_name": "Use Management",
                "short_desc": "Auto-discover addresses via AMQP Management Protocol",
                "type": AbstractDataProviderType::get(SoftBoolType),
                "desc": "If `True`, queries the broker via the AMQP Management Protocol "
                    "to auto-discover available addresses",
                "default_value": False,
            },
            "username": <DataProviderOptionInfo>{
                "display_name": "Username",
                "short_desc": "SASL username",
                "type": AbstractDataProviderType::get(StringType),
                "desc": "Username for SASL authentication",
            },
            "password": <DataProviderOptionInfo>{
                "display_name": "Password",
                "short_desc": "SASL password",
                "type": AbstractDataProviderType::get(StringType),
                "desc": "Password for SASL authentication",
                "sensitive": True,
            },
            "timeout_ms": <DataProviderOptionInfo>{
                "display_name": "Timeout",
                "short_desc": "Default timeout in milliseconds",
                "type": AbstractDataProviderType::get(SoftIntType),
                "desc": "Default timeout for AMQP operations in milliseconds",
                "default_value": 30000,
            },
        };
    }

    private {
        #! The AMQP client
        Amqp::AmqpClient m_client;

        #! Known addresses
        list<string> m_addresses = ();

        #! Default timeout
        int m_timeout_ms = 30000;

        #! Display name
        string m_display_name = "AMQP";
    }

    #! Creates the root AMQP data provider from constructor options
    /** @param options constructor options hash

        @throw CONSTRUCTOR-ERROR missing or invalid options
    */
    constructor(*hash<auto> options) {
        *hash<auto> copts = checkOptions("CONSTRUCTOR-ERROR", ConstructorOptions, options);

        if (!copts.url) {
            throw "CONSTRUCTOR-ERROR", "'url' option is required";
        }

        # Build connection options
        string url_str = copts.url.toString();
        hash<AmqpConnectionOptions> conn_opts({"url": url_str});
        if (copts.username || copts.password) {
            conn_opts.sasl = cast<hash<AmqpSaslOptions>>({
                "mechanism": "PLAIN",
                "username": copts.username.toString(),
                "password": copts.password.toString(),
            });
        }

        m_client = new Amqp::AmqpClient(conn_opts);
        m_client.connect();

        if (copts.timeout_ms) {
            m_timeout_ms = copts.timeout_ms;
        }

        # Discover addresses
        if (copts.addresses) {
            m_addresses = copts.addresses.split(",");
            m_addresses = map trim($1), m_addresses;
        }

        if (copts.use_management) {
            try {
                Amqp::AmqpManagementClient mgmt = m_client.getManagementClient();
                list<hash<AmqpAddressInfo>> broker_addrs = mgmt.queryAddresses();
                for (int i = 0; i < broker_addrs.size(); ++i) {
                    string addr_name = broker_addrs[i].name;
                    if (!inlist(addr_name, m_addresses)) {
                        m_addresses += addr_name;
                    }
                }
            } catch (hash<ExceptionInfo> ex) {
                # Gracefully ignore if management not supported
            }
        }

        m_display_name = sprintf("AMQP %s", copts.url);
    }

    #! Creates the root AMQP data provider from a pre-built client
    /** @param client the AMQP client (must already be connected)
        @param addresses list of addresses to expose
        @param timeout_ms default timeout
    */
    constructor(Amqp::AmqpClient client, list<string> addresses, int timeout_ms = 30000) {
        m_client = client;
        m_addresses = addresses;
        m_timeout_ms = timeout_ms;
    }

    #! Returns the data provider name
    string getName() {
        return m_display_name;
    }

    #! Returns the data provider description
    *string getDesc() {
        return sprintf("AMQP data provider with %d address(es)", m_addresses.size());
    }

    #! Returns child provider names (address names)
    private *list<string> getChildProviderNamesImpl() {
        return m_addresses;
    }

    #! Returns a child provider by name (address provider)
    private *AbstractDataProvider getChildProviderImpl(string name) {
        if (!inlist(name, m_addresses)) {
            return NOTHING;
        }
        return new AmqpAddressDataProvider(m_client, name, m_timeout_ms);
    }

    #! Returns static provider info
    private hash<DataProviderInfo> getStaticInfoImpl() {
        return ProviderInfo;
    }
}

} # namespace AmqpDataProvider
