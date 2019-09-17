import dynamo from "local-dynamo";
import * as t from "io-ts";
import sleep from "sleep-promise";
import _ from "lodash";
import aws from "aws-sdk";

import tynogels from "./index";

const shouldLog = false;

aws.config.update({
  accessKeyId: "YOURKEY",
  secretAccessKey: "YOURSECRET"
});

tynogels.config({
  region: "us-east-1",
  endpoint: "http://localhost:8000"
});

const region = "us-east-1";
const ddb = new aws.DynamoDB({ region, endpoint: "http://localhost:8000" });

const typeToAttribute = (type: t.Any) =>
  ({
    string: "S",
    number: "N"
  }[type.name]);

const createTable = async config => {
  try {
    return ddb
      .createTable(
        (([hashKey, sortKey]) => ({
          TableName: config.tableName,
          AttributeDefinitions: _.filter([
            {
              AttributeName: hashKey,
              AttributeType: typeToAttribute(config.hashKey[hashKey])
            },
            sortKey
              ? {
                  AttributeName: sortKey,
                  AttributeType: typeToAttribute(config.sortKey[sortKey])
                }
              : null
          ]),
          KeySchema: _.filter([
            {
              AttributeName: hashKey,
              KeyType: "HASH"
            },
            sortKey
              ? {
                  AttributeName: sortKey,
                  KeyType: "RANGE"
                }
              : null
          ]),
          BillingMode: "PAY_PER_REQUEST",
          ProvisionedThroughput: {
            ReadCapacityUnits: 100,
            WriteCapacityUnits: 100
          }
        }))(_.keys({ ...config.hashKey, ...config.sortKey }))
      )
      .promise();
  } catch (error) {
    console.log(error);
  }
};

const User = tynogels.define({
  hashKey: {
    name: t.string
  },
  sortKey: {
    age: t.number
  },
  schema: {
    name: t.string,
    age: t.number,
    email: t.string,
    thing: t.string
  },
  tableName: "users",
  secondaryIndexes: []
});

const Building = tynogels.define({
  tableName: "buildings",
  hashKey: {
    buildingId: t.number
  },
  sortKey: {
    location: t.string
  },
  schema: {
    buildingId: t.number,
    location: t.string
  },
  secondaryIndexes: []
});

const dynProcess = dynamo.launch(null, 8000);

createTable(User.config);
createTable(Building.config);

// Create "Movies" table with GSI with only hash key

ddb
  .createTable({
    TableName: "Movies",
    KeySchema: [
      { AttributeName: "year", KeyType: "HASH" } //Partition key
    ],
    AttributeDefinitions: [
      { AttributeName: "year", AttributeType: "N" },
      { AttributeName: "filmCode", AttributeType: "N" }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 10,
      WriteCapacityUnits: 10
    },
    GlobalSecondaryIndexes: [
      {
        IndexName: "Gsi-Test",
        KeySchema: [{ AttributeName: "filmCode", KeyType: "HASH" }],
        Projection: {
          ProjectionType: "KEYS_ONLY"
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 10,
          WriteCapacityUnits: 10
        }
      }
    ]
  })
  .promise();

ddb
  .createTable({
    TableName: "Books",
    KeySchema: [{ AttributeName: "year", KeyType: "HASH" }],
    AttributeDefinitions: [
      { AttributeName: "year", AttributeType: "N" },
      { AttributeName: "bookCode", AttributeType: "N" },
      { AttributeName: "name", AttributeType: "S" }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 10,
      WriteCapacityUnits: 10
    },
    GlobalSecondaryIndexes: [
      {
        IndexName: "Gsi-Test",
        KeySchema: [
          { AttributeName: "bookCode", KeyType: "HASH" },
          { AttributeName: "year", KeyType: "RANGE" }
        ],
        Projection: {
          ProjectionType: "KEYS_ONLY"
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 10,
          WriteCapacityUnits: 10
        }
      },
      {
        IndexName: "Foo-Test",
        KeySchema: [
          { AttributeName: "bookCode", KeyType: "HASH" },
          { AttributeName: "name", KeyType: "RANGE" }
        ],
        Projection: {
          ProjectionType: "KEYS_ONLY"
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 10,
          WriteCapacityUnits: 10
        }
      }
    ]
  })
  .promise();

jest.setTimeout(30000);

beforeAll(async () => sleep(20000));
afterEach(async () => sleep(100));
afterAll(() => dynProcess.kill());

it("Create user", async () => {
  await User.create({
    name: "foo",
    age: 18
  });
});

it("Batch create users", async () => {
  await User.create([
    ..._.times(50, i => ({
      name: `foo${i}`,
      email: `foo${i}@bar`,
      age: 18,
      thing: "foob"
    })),
    ..._.times(30, i => ({
      name: "foob",
      age: i + 1,
      thing: `foob_${i + 1}`
    }))
  ]);
});

it("Update user", async () => {
  await User.update({
    name: "foo",
    age: 18,
    thing: "what"
  });
});

it("Delete user", async () => {
  await User.delete({
    name: "foo",
    age: 18
  });
});

it("Read user", async () => {
  const user = await User.read({
    name: "foo1",
    age: 18
  });

  if (shouldLog) {
    console.log(user);
  }
});

it("Batch read user", async () => {
  const users = await User.batchRead(
    _.times(50, i => ({
      name: `foo${i}`,
      age: 18
    }))
  );

  if (shouldLog) {
    console.log(users);
  }
});

it("Simple query user", async () => {
  const users = await User.query("foob").exec();

  if (shouldLog) {
    console.log(users);
  }
});

it("Query on sort key equality", async () => {
  const users = await User.query("foob")
    .where("age")
    .equals(15)
    .exec();

  if (shouldLog) {
    console.log(users);
  }
});

it("Query on sort key via less than or equal", async () => {
  const users = await User.query("foob")
    .where("age")
    .lte(10)
    .exec();

  if (shouldLog) {
    console.log(users);
  }
});

it("Query on sort key via less than", async () => {
  const users = await User.query("foob")
    .where("age")
    .lt(6)
    .exec();

  if (shouldLog) {
    console.log(users);
  }
});

it("Query on sort key via greater than", async () => {
  const users = await User.query("foob")
    .where("age")
    .gt(24)
    .exec();

  if (shouldLog) {
    console.log(users);
  }
});

it("Query on sort key via greater than or equal", async () => {
  const users = await User.query("foob")
    .where("age")
    .gte(15)
    .exec();

  if (shouldLog) {
    console.log(users);
  }
});

it("Query sort keys that are between two values", async () => {
  const users = await User.query("foob")
    .where("age")
    .between(5, 10)
    .exec();

  if (shouldLog) {
    console.log(users);
  }
});

it("Create building record", async () => {
  await Building.create({
    buildingId: 13371337,
    location: "The Moon"
  });
});

it("Batch create building records", async () => {
  await Building.create([
    {
      buildingId: 100,
      location: "United States, New York"
    },
    {
      buildingId: 100,
      location: "United States, New Jersey"
    },
    {
      buildingId: 100,
      location: "Australia, Sydney"
    }
  ]);
});

it("Query sort key that begins with a value", async () => {
  const buildings = await Building.query(100)
    .where("location")
    .beginsWith("United States")
    .exec();

  if (shouldLog) {
    console.log(buildings);
  }
});

const Movie = tynogels.define({
  tableName: "Movies",
  hashKey: {
    year: t.number
  },
  sortKey: {},
  schema: {
    year: t.number,
    filmCode: t.number
  },
  secondaryIndexes: [
    {
      name: t.literal("Gsi-Test"),
      hashKey: {
        filmCode: t.number
      },
      sortKey: {},
      schema: {
        filmCode: t.number
      }
    }
  ]
});

it("Create movies for later testing", async () => {
  await Movie.create({ year: 1000, filmCode: 10 });
  await Movie.create({ year: 2000, filmCode: 20 });
  await Movie.create({ year: 3000, filmCode: 30 });
  await Movie.create({ year: 4000, filmCode: 40 });
});

it("Read movie based on GSI", async () => {
  const movie = await Movie.index("Gsi-Test")
    .query(10)
    .exec();

  if (shouldLog) {
    console.log(movie);
  }
});

const secondaryIndexes = [
  {
    name: t.literal("Gsi-Test"),
    hashKey: {
      bookCode: t.number
    },
    sortKey: {
      year: t.number
    },
    schema: {
      bookCode: t.number,
      year: t.number,
      whatever: t.string
    }
  },
  {
    name: t.literal("Foo-Test"),
    hashKey: {
      bookCode: t.number
    },
    sortKey: {
      name: t.string
    },
    schema: {
      bookCode: t.number,
      name: t.string
    }
  }
] as const;

const Book = tynogels.define({
  tableName: "Books",
  hashKey: {
    year: t.number
  },
  sortKey: {
    foob: t.string
  },
  schema: {
    year: t.number,
    foob: t.string,
    bookCode: t.number,
    random: t.string,
    name: t.string
  },
  secondaryIndexes
});

it("Create books for later testing", async () => {
  await Promise.all(
    _.times(5, x => Book.create({ year: 1000 + x, bookCode: x + 42 }))
  );

  const names = ["aaa", "abc/", "ccc", "ddd", "eee"];

  await Promise.all(
    _.times(5, x =>
      Book.create({ year: 2000 + x, bookCode: 1337, name: names[x] })
    )
  );
});

it("Query books simply on hash key", async () => {
  const books = await Book.query(1000).exec();

  if (shouldLog) {
    console.log(books);
  }
});

it("Simple query using index", async () => {
  const books = await Book.index("Gsi-Test")
    .query(1337)
    .exec();

  if (shouldLog) {
    console.log(books);
  }
});

it("Hash/sort index query via equals", async () => {
  const books = await Book.index("Gsi-Test")
    .query(1337)
    .where("year")
    .equals(2002)
    .exec();

  if (shouldLog) {
    console.log(books);
  }
});

it("Hash/sort index query via less than or equal", async () => {
  const books = await Book.index("Gsi-Test")
    .query(1337)
    .where("year")
    .lte(2002)
    .exec();

  if (shouldLog) {
    console.log(books);
  }
});

it("Hash/sort index query via less than", async () => {
  const books = await Book.index("Gsi-Test")
    .query(1337)
    .where("year")
    .lt(2002)
    .exec();

  if (shouldLog) {
    console.log(books);
  }
});

it("Hash/sort index query via greater than", async () => {
  const books = await Book.index("Gsi-Test")
    .query(1337)
    .where("year")
    .gt(2002)
    .exec();

  if (shouldLog) {
    console.log(books);
  }
});

it("Hash/sort index query via greater than or equal", async () => {
  const books = await Book.index("Gsi-Test")
    .query(1337)
    .where("year")
    .gte(2002)
    .exec();

  if (shouldLog) {
    console.log(books);
  }
});

it("Hash/sort index query via between", async () => {
  const books = await Book.index("Gsi-Test")
    .query(1337)
    .where("year")
    .between(2002, 2003)
    .exec();

  if (shouldLog) {
    console.log(books);
  }
});
