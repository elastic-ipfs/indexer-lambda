/* IMPORTANT
 * This snapshot file is auto-generated, but designed for humans.
 * It should be checked into source control and tracked carefully.
 * Re-generate by setting TAP_SNAPSHOT=1 and running tests.
 * Make sure to inspect the output below.  Do not ignore changes!
 */
'use strict'
exports[`test/index.test.js TAP handler indexes a new car file > must match snapshot 1`] = `
Object {
  "dynamo": Object {
    "batchCreates": Array [
      Object {
        "RequestItems": Object {
          "v1-blocks": Array [
            Object {
              "PutRequest": Object {
                "Item": Object {
                  "createdAt": Object {
                    "S": "2022-06-24T15:15:17.401Z",
                  },
                  "multihash": Object {
                    "S": "zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC",
                  },
                  "type": Object {
                    "S": "raw",
                  },
                },
              },
            },
            Object {
              "PutRequest": Object {
                "Item": Object {
                  "createdAt": Object {
                    "S": "2022-06-24T15:15:17.401Z",
                  },
                  "multihash": Object {
                    "S": "zQmSGtsqx7aYH8gP21AgidxXuX5vsseFJgHKa75kg8HepXL",
                  },
                  "type": Object {
                    "S": "dag-pb",
                  },
                },
              },
            },
            Object {
              "PutRequest": Object {
                "Item": Object {
                  "createdAt": Object {
                    "S": "2022-06-24T15:15:17.401Z",
                  },
                  "multihash": Object {
                    "S": "zQmSHc8o3PxQgMccYgGtuStaNQKXTBX1rTHN5W9cUCwrcHX",
                  },
                  "type": Object {
                    "S": "dag-pb",
                  },
                },
              },
            },
            Object {
              "PutRequest": Object {
                "Item": Object {
                  "createdAt": Object {
                    "S": "2022-06-24T15:15:17.401Z",
                  },
                  "multihash": Object {
                    "S": "zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339",
                  },
                  "type": Object {
                    "S": "dag-pb",
                  },
                },
              },
            },
          ],
          "v1-blocks-cars-position": Array [
            Object {
              "PutRequest": Object {
                "Item": Object {
                  "blockmultihash": Object {
                    "S": "zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC",
                  },
                  "carpath": Object {
                    "S": "us-east-2/cars/file1.car",
                  },
                  "length": Object {
                    "N": "4",
                  },
                  "offset": Object {
                    "N": "96",
                  },
                },
              },
            },
            Object {
              "PutRequest": Object {
                "Item": Object {
                  "blockmultihash": Object {
                    "S": "zQmSGtsqx7aYH8gP21AgidxXuX5vsseFJgHKa75kg8HepXL",
                  },
                  "carpath": Object {
                    "S": "us-east-2/cars/file1.car",
                  },
                  "length": Object {
                    "N": "51",
                  },
                  "offset": Object {
                    "N": "137",
                  },
                },
              },
            },
            Object {
              "PutRequest": Object {
                "Item": Object {
                  "blockmultihash": Object {
                    "S": "zQmSHc8o3PxQgMccYgGtuStaNQKXTBX1rTHN5W9cUCwrcHX",
                  },
                  "carpath": Object {
                    "S": "us-east-2/cars/file1.car",
                  },
                  "length": Object {
                    "N": "51",
                  },
                  "offset": Object {
                    "N": "225",
                  },
                },
              },
            },
            Object {
              "PutRequest": Object {
                "Item": Object {
                  "blockmultihash": Object {
                    "S": "zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339",
                  },
                  "carpath": Object {
                    "S": "us-east-2/cars/file1.car",
                  },
                  "length": Object {
                    "N": "51",
                  },
                  "offset": Object {
                    "N": "313",
                  },
                },
              },
            },
          ],
        },
      },
      Object {
        "RequestItems": Object {
          "v1-blocks": Array [
            Object {
              "PutRequest": Object {
                "Item": Object {
                  "createdAt": Object {
                    "S": "2022-06-24T15:15:17.401Z",
                  },
                  "multihash": Object {
                    "S": "zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn",
                  },
                  "type": Object {
                    "S": "dag-pb",
                  },
                },
              },
            },
          ],
          "v1-blocks-cars-position": Array [
            Object {
              "PutRequest": Object {
                "Item": Object {
                  "blockmultihash": Object {
                    "S": "zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn",
                  },
                  "carpath": Object {
                    "S": "us-east-2/cars/file1.car",
                  },
                  "length": Object {
                    "N": "4",
                  },
                  "offset": Object {
                    "N": "401",
                  },
                },
              },
            },
          ],
        },
      },
    ],
    "creates": Array [
      Object {
        "Item": Object {
          "bucket": Object {
            "S": "cars",
          },
          "bucketRegion": Object {
            "S": "us-east-2",
          },
          "createdAt": Object {
            "S": "2022-06-24T15:15:17.401Z",
          },
          "fileSize": Object {
            "N": "148",
          },
          "key": Object {
            "S": "file1.car",
          },
          "path": Object {
            "S": "us-east-2/cars/file1.car",
          },
          "roots": Object {
            "L": Array [
              Object {
                "S": "bafybeib2u4mc4vsgpxp7fktmhpg5ncdviinwpfigagndcek43tde7uak2i",
              },
            ],
          },
          "version": Object {
            "N": "1",
          },
        },
        "TableName": "v1-cars",
      },
    ],
  },
  "sqs": Object {
    "batchPublishes": Array [
      Object {
        "Entries": Array [
          Object {
            "Id": "zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC",
            "MessageBody": "zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC",
          },
          Object {
            "Id": "zQmSGtsqx7aYH8gP21AgidxXuX5vsseFJgHKa75kg8HepXL",
            "MessageBody": "zQmSGtsqx7aYH8gP21AgidxXuX5vsseFJgHKa75kg8HepXL",
          },
          Object {
            "Id": "zQmSHc8o3PxQgMccYgGtuStaNQKXTBX1rTHN5W9cUCwrcHX",
            "MessageBody": "zQmSHc8o3PxQgMccYgGtuStaNQKXTBX1rTHN5W9cUCwrcHX",
          },
          Object {
            "Id": "zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339",
            "MessageBody": "zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339",
          },
        ],
        "QueueUrl": "publishingQueue",
      },
      Object {
        "Entries": Array [
          Object {
            "Id": "zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn",
            "MessageBody": "zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn",
          },
        ],
        "QueueUrl": "publishingQueue",
      },
    ],
    "publishes": Array [
      Object {
        "MessageBody": "us-east-2/cars/file1.car",
        "QueueUrl": "notificationsQueue",
      },
    ],
  },
}
`

exports[`test/index.test.js TAP handler indexes a new car file with unsupported blocks > must match snapshot 1`] = `
Object {
  "dynamo": Object {
    "batchCreates": Array [
      Object {
        "RequestItems": Object {
          "v1-blocks": Array [
            Object {
              "PutRequest": Object {
                "Item": Object {
                  "createdAt": Object {
                    "S": "2022-06-24T15:15:17.401Z",
                  },
                  "multihash": Object {
                    "S": "zQmPH3Su9xAqw4WRbXT6DvwNpmaXYvTKKAY2hBKJsC7j2b4",
                  },
                  "type": Object {
                    "S": "unsupported",
                  },
                },
              },
            },
          ],
          "v1-blocks-cars-position": Array [
            Object {
              "PutRequest": Object {
                "Item": Object {
                  "blockmultihash": Object {
                    "S": "zQmPH3Su9xAqw4WRbXT6DvwNpmaXYvTKKAY2hBKJsC7j2b4",
                  },
                  "carpath": Object {
                    "S": "us-east-2/cars/file2.car",
                  },
                  "length": Object {
                    "N": "8",
                  },
                  "offset": Object {
                    "N": "96",
                  },
                },
              },
            },
          ],
        },
      },
    ],
    "creates": Array [
      Object {
        "Item": Object {
          "bucket": Object {
            "S": "cars",
          },
          "bucketRegion": Object {
            "S": "us-east-2",
          },
          "createdAt": Object {
            "S": "2022-06-24T15:15:17.401Z",
          },
          "fileSize": Object {
            "N": "148",
          },
          "key": Object {
            "S": "file2.car",
          },
          "path": Object {
            "S": "us-east-2/cars/file2.car",
          },
          "roots": Object {
            "L": Array [
              Object {
                "S": "baerreian54jmxresipslpi3gybouz5tds5tzbjomhehfybpp34v3v6zuvm",
              },
            ],
          },
          "version": Object {
            "N": "1",
          },
        },
        "TableName": "v1-cars",
      },
    ],
  },
  "sqs": Object {
    "batchPublishes": Array [
      Object {
        "Entries": Array [
          Object {
            "Id": "zQmPH3Su9xAqw4WRbXT6DvwNpmaXYvTKKAY2hBKJsC7j2b4",
            "MessageBody": "zQmPH3Su9xAqw4WRbXT6DvwNpmaXYvTKKAY2hBKJsC7j2b4",
          },
        ],
        "QueueUrl": "publishingQueue",
      },
    ],
    "publishes": Array [
      Object {
        "MessageBody": "us-east-2/cars/file2.car",
        "QueueUrl": "notificationsQueue",
      },
    ],
  },
}
`
