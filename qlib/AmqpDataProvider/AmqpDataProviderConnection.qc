# -*- mode: qore; indent-tabs-mode: nil -*-
#! @file AmqpDataProviderConnection.qc Connection class for AMQP

/*  AmqpDataProviderConnection.qc Copyright 2026 Qore Technologies, s.r.o.

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

#! Connection class for AMQP brokers
/** Supports \c amqp:// (plaintext) and \c amqps:// (TLS) URL schemes.

*/
public class AmqpDataProviderConnection inherits ConnectionProvider::AbstractConnectionWithInfo {
    public {
        #! Connection scheme info
        const ConnectionScheme = <ConnectionSchemeInfo>{
            "display_name": "AMQP Connection",
            "short_desc": "A connection to an AMQP 1.0 message broker",
            "desc": "A connection to an AMQP 1.0 message broker for sending and "
                "receiving messages with support for message properties, delivery "
                "disposition, and broker introspection",
            "cls": Class::forName("AmqpDataProvider::AmqpDataProviderConnection"),
            "schemes": {
                "amqp": True,
                "amqps": True,
            },
            "base_scheme_map": {
                "amqp": "amqp",
                "amqps": "amqps",
            },
            "options": {
                "addresses": <ConnectionOptionInfo>{
                    "display_name": "Addresses",
                    "short_desc": "Comma-separated list of AMQP addresses",
                    "type": "string",
                    "desc": "Comma-separated list of AMQP addresses to expose as "
                        "child providers",
                },
                "use_management": <ConnectionOptionInfo>{
                    "display_name": "Use Management",
                    "short_desc": "Auto-discover addresses via AMQP Management Protocol",
                    "type": "bool",
                    "desc": "If `True`, queries the broker to auto-discover available "
                        "addresses",
                    "default_value": False,
                },
                "timeout_ms": <ConnectionOptionInfo>{
                    "display_name": "Timeout",
                    "short_desc": "Default timeout in milliseconds",
                    "type": "int",
                    "desc": "Default timeout for AMQP operations in milliseconds",
                    "default_value": 30000,
                },
            },
        };
    }

    #! Creates the AmqpDataProviderConnection object
    /** @param name the connection name
        @param description connection description
        @param url the connection URL (amqp:// or amqps://)
        @param attributes optional connection attributes
        @param options connection options
    */
    constructor(string name, string description, string url, hash<auto> attributes = {},
            hash<auto> options = {})
            : AbstractConnectionWithInfo(name, description, url, attributes, options) {
    }

    #! Creates the AmqpDataProviderConnection object from a config hash
    /** @param config connection configuration hash
        @param attr optional connection attributes
    */
    constructor(hash<auto> config, *hash<auto> attr) : AbstractConnectionWithInfo(config, attr) {
    }

    #! Returns an AmqpDataProvider for this connection
    DataProvider::AbstractDataProvider getDataProvider() {
        hash<auto!> provider_opts({"url": url});
        if (self.opts.addresses) {
            provider_opts.addresses = self.opts.addresses;
        }
        if (self.opts.use_management) {
            provider_opts.use_management = self.opts.use_management;
        }
        if (self.opts.timeout_ms) {
            provider_opts.timeout_ms = self.opts.timeout_ms;
        }
        return new AmqpDataProvider(provider_opts);
    }

    #! Returns "amqp"
    string getType() {
        return "amqp";
    }

    #! Returns the ConnectionSchemeInfo for this connection
    private hash<ConnectionSchemeInfo> getConnectionSchemeInfoImpl() {
        return ConnectionScheme;
    }

    #! Returns the underlying connection object
    private Amqp::AmqpClient getImpl(bool connect = True, *hash<auto> rtopts) {
        Amqp::AmqpClient client(url);
        if (connect) {
            client.connect();
        }
        return client;
    }

    #! Returns a ping hash for the connection
    private hash<PingInfo> pingImpl() {
        date start = now_us();
        try {
            Amqp::AmqpClient client(url);
            client.connect();
            client.close();
            return <PingInfo>{
                "ok": True,
                "time": now_us() - start,
                "info": "OK",
            };
        } catch (hash<ExceptionInfo> ex) {
            return <PingInfo>{
                "ok": False,
                "time": now_us() - start,
                "info": sprintf("%s: %s", ex.err, ex.desc),
            };
        }
    }
}

} # namespace AmqpDataProvider
