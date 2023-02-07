local validTableNamePattern = '^[a-zA-Z][a-zA-Z0-9]+$';


local Base = {
  title: 'yayam yaml database schema',
  version: '2023-02-01',
  parents: [],  // empty = origin schema
  type: 'object',
  description: 'json schema referenced by data in CacheableDataResult',
  properties: {
    _schemas: {
      type: 'object',
      patternProperties: {
        [validTableNamePattern]: {
          type: 'object',
          properties: {
            type: {
              description: 'table rows are arrays',
              type: 'string',
              pattern: 'array',
            },
            items: {
              type: 'object',
              properties: {
                type: {
                  description: 'schema for a single row, which is an object',
                  type: 'string',
                  pattern: 'object',
                },
                properties: {
                  type: 'object',
                  // FIXME: assert the properties specify the schema for the rows
                },
              },
            },
          },
        },
      },
    },
    _keys: {
      type: ['object', 'null'],
    },
    _codecs: {
      type: ['object', 'null'],
    },

  },
  patternProperties: {
    [validTableNamePattern]: {
      type: ['array', 'null'],
    },
  },
};

Base
