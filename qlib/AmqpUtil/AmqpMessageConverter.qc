# -*- mode: qore; indent-tabs-mode: nil -*-
#! @file AmqpMessageConverter.qc Automatic message body serialization/deserialization

/*  AmqpMessageConverter.qc Copyright 2026 Qore Technologies, s.r.o.

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

#! Main Amqp namespace
public namespace Amqp {

#! Automatic message body serialization based on content_type
/** Encodes and decodes message bodies to/from JSON, YAML, and XML based on
    the message's content_type property. Modules are loaded optionally via
    \c %try-module in the AmqpUtil.qm; runtime checks verify availability.

    @par Supported Content Types
    - \c application/json — requires \c json module
    - \c application/yaml, \c text/yaml — requires \c yaml module
    - \c application/xml, \c text/xml — requires \c xml module
    - All other types — passed through unchanged
*/
public class AmqpMessageConverter {
    #! Encode a body value based on content_type
    /** @param body the value to encode
        @param content_type the MIME content type

        @return the encoded body (string or binary)

        @throw AMQP-CONVERTER-ERROR if the required module is not available
    */
    static auto encode(auto body, string content_type) {
        switch (content_type) {
            case "application/json":
                return AmqpMessageConverter::callJsonEncode(body);

            case "application/yaml":
            case "text/yaml":
                return AmqpMessageConverter::callYamlEncode(body);

            case "application/xml":
            case "text/xml":
                return AmqpMessageConverter::callXmlEncode(body);
        }
        return body;
    }

    #! Decode a message body based on content_type
    /** @param body the raw body to decode
        @param content_type the MIME content type (NOTHING = no conversion)

        @return the decoded value

        @throw AMQP-CONVERTER-ERROR if the required module is not available
    */
    static auto decode(auto body, *string content_type) {
        if (!content_type || body.typeCode() != NT_STRING) {
            return body;
        }
        switch (content_type) {
            case "application/json":
                return AmqpMessageConverter::callJsonDecode(body);

            case "application/yaml":
            case "text/yaml":
                return AmqpMessageConverter::callYamlDecode(body);

            case "application/xml":
            case "text/xml":
                return AmqpMessageConverter::callXmlDecode(body);
        }
        return body;
    }

    # Runtime-checked encoding/decoding using call_function to avoid parse-time errors
    private static auto callJsonEncode(auto body) {
        try {
            return call_function("make_json", body);
        } catch (hash<ExceptionInfo> ex) {
            if (ex.err == "NO-FUNCTION") {
                throw "AMQP-CONVERTER-ERROR", "json module is required for application/json encoding";
            }
            rethrow;
        }
    }

    private static auto callJsonDecode(auto body) {
        try {
            return call_function("parse_json", body);
        } catch (hash<ExceptionInfo> ex) {
            if (ex.err == "NO-FUNCTION") {
                throw "AMQP-CONVERTER-ERROR", "json module is required for application/json decoding";
            }
            rethrow;
        }
    }

    private static auto callYamlEncode(auto body) {
        try {
            return call_function("make_yaml", body);
        } catch (hash<ExceptionInfo> ex) {
            if (ex.err == "NO-FUNCTION") {
                throw "AMQP-CONVERTER-ERROR", "yaml module is required for YAML encoding";
            }
            rethrow;
        }
    }

    private static auto callYamlDecode(auto body) {
        try {
            return call_function("parse_yaml", body);
        } catch (hash<ExceptionInfo> ex) {
            if (ex.err == "NO-FUNCTION") {
                throw "AMQP-CONVERTER-ERROR", "yaml module is required for YAML decoding";
            }
            rethrow;
        }
    }

    private static auto callXmlEncode(auto body) {
        try {
            return call_function("make_xml", body);
        } catch (hash<ExceptionInfo> ex) {
            if (ex.err == "NO-FUNCTION") {
                throw "AMQP-CONVERTER-ERROR", "xml module is required for XML encoding";
            }
            rethrow;
        }
    }

    private static auto callXmlDecode(auto body) {
        try {
            return call_function("parse_xml", body);
        } catch (hash<ExceptionInfo> ex) {
            if (ex.err == "NO-FUNCTION") {
                throw "AMQP-CONVERTER-ERROR", "xml module is required for XML decoding";
            }
            rethrow;
        }
    }
}

} # namespace Amqp
