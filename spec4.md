🧾 Specification: CommonPayloadData YAML Line Format




Format

Each line in the YAML file must be a YAML flow-style array, containing exactly 4 positional fields:

- [<timestamp>, <topic>, <categories>, <payload_object>]

Field Definitions

    Timestamp (float64)

        UNIX timestamp in seconds (with fractional precision allowed)

        Example: 1718065243.123

    Topic (string)

        Full MQTT topic string

        Example: "sensors/foo/bar/1"

    Categories ([]int)

        List of category IDs (foreign keys into category join table)

        Can be empty ([])

        Must always be present

        Example: [3,6,23]

    Payload Object (*orderedmapjson.AnyOrderedMap)

        A flat key-value map serialized as a flow-style YAML object

        Must always be the final item

        Keys and values are both strings

        Field order must be preserved during parsing

        Example: {name: "something", event: "whatever"}

Constraints

    The YAML list must contain at most 4 elements.

    The 4th element, if present, is parsed as an object; it can also be `null`.

    The object must be parsed using orderedmapjson.AnyOrderedMap to preserve key order.

    The YAML list must be a single line; newlines must be escaped.

    Example valid YAML line:

    - [1718065243.123, "sensors/foo/bar/1", [3,6,23], {event: "boot", name: "sensor-42"}]

Parser Implementation Notes (Go)

    Use any standard YAML or JSON parser to deserialize the outer array, if possible -- but the final object must be parsed using the internal, order-preserving, object parser that yields orderedmapjson.AnyOrderMap.

    Ensure the parser allows manual handling of the final array element:

        Use native parser to extract the first 3 fields.

        Serialize the 4th element (the object) as a raw byte slice or yaml.Node.

        Pass that object to orderedmapjson.UnmarshalJSON/YAML to preserve key order.

    If exact unmarshaling can't be done shallowly, consider a two-pass parse: first deserialize the array, then manually re-parse the object portion.


# more spec

consider commonpayloaddata's jsonl and cpd.yaml versions

we now need bidirectional functions for the data in these files.
for yaml -> jsonl this is a recovery.
per the spec, every row under data: is a 3-element array. time, tag, payload.
to recover jsonl, we extract the components.
- time: as-is
- tags: ints become strings via the table lookup. in the yaml, the table lookup MUST be exhaustive
- payload is used as-is, but as shown in the jsonl, is "expanded" to the top level along with "time" and "tags"

we expect to expand the yaml data: section into the rows as in the jsonl version

for jsonl -> yaml
- require "time" key and extract it into the first element in the array
- extract "tags" if present into array; empty array if not present
- first pass should read through all tags from the jsonl; tags are expected to be array of strings
  - based on order of appearance of the string tags, build the lookup table of string->int where int is the 1-based autoincrement id
  - second pass, for each row, convert every string tag to the id
- store the lookup table under the `tags:` entry in the yaml
- `data:` MUST BE the final entry in the yaml
- every row from the jsonl becomes 1 row of an array under `data:`

