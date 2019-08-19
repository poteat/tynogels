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
    email: t.string,
    thing: t.string
  },
  tableName: "users"
});

const Building = tynogels.define({
  tableName: "buildings",
  hashKey: {
    buildingId: t.number
  },
  sortKey: {
    location: t.string
  },
  schema: {}
});

const dynProcess = dynamo.launch(null, 8000);

createTable(User.config);
createTable(Building.config);

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
