import * as t from "io-ts";
import aws from "aws-sdk";
import _ from "lodash";

const region = "us-east-1";
let doc = new aws.DynamoDB.DocumentClient({ region });

export default {
  config: (config: AWS.DynamoDB.ClientConfiguration) =>
    (doc = new aws.DynamoDB.DocumentClient(config)),
  define: <P1 extends t.Props, P2 extends t.Props, P3 extends t.Props>(config: {
    tableName: string;
    hashKey: P1;
    sortKey: P2;
    schema: P3;
  }) =>
    ((type, keyType, hashType, sortType) => ({
      config,
      create: async (
        x:
          | (t.TypeOf<typeof type> & t.TypeOf<typeof keyType>)
          | (t.TypeOf<typeof type> & t.TypeOf<typeof keyType>)[]
      ) =>
        _.isArray(x)
          ? Promise.all(
              _.chunk(x, 25).map(x =>
                doc
                  .batchWrite({
                    RequestItems: {
                      [config.tableName]: x.map(x => ({
                        PutRequest: { Item: x }
                      }))
                    }
                  })
                  .promise()
              )
            )
          : doc.put({ Item: x, TableName: config.tableName }).promise(),
      read: async (
        x: t.TypeOf<typeof keyType>
      ): Promise<t.TypeOf<typeof keyType> & t.TypeOf<typeof type>> =>
        (await doc.get({ Key: x, TableName: config.tableName }).promise())
          .Item as any,
      batchRead: async (
        x: t.TypeOf<typeof keyType>[]
      ): Promise<(t.TypeOf<typeof keyType> & t.TypeOf<typeof type>)[]> =>
        (await Promise.all(
          _.chunk(x, 25).map(x =>
            doc
              .batchGet({
                RequestItems: {
                  [config.tableName]: { Keys: x }
                }
              })
              .promise()
          )
        )).flatMap(x => x.Responses[config.tableName]) as any,
      update: async (x: t.TypeOf<typeof type> & t.TypeOf<typeof keyType>) =>
        doc
          .update(
            (key => ({
              TableName: config.tableName,
              Key: _.pick(x, key),
              ...(x => ({
                UpdateExpression: `set ${_.map(
                  x,
                  (x, i) => `${i}=:${i}`
                ).join()}`,
                ExpressionAttributeValues: _.mapKeys(
                  x as any,
                  (x, i) => `:${i}`
                )
              }))(_.omit(x, key))
            }))(_.keys({ ...config.hashKey, ...config.sortKey }))
          )
          .promise(),
      query: (
        hashValue: t.TypeOf<typeof hashType>[keyof t.TypeOf<typeof hashType>]
      ) => ({
        exec: async (): Promise<
          (t.TypeOf<typeof keyType> & t.TypeOf<typeof type>)[]
        > =>
          (await doc
            .query({
              TableName: config.tableName,
              KeyConditionExpression: `#x = :x`,
              ExpressionAttributeNames: {
                "#x": _.keys(config.hashKey)[0]
              },
              ExpressionAttributeValues: {
                ":x": hashValue
              }
            })
            .promise()).Items as any,
        where: (sortKey: keyof t.TypeOf<typeof sortType>) =>
          (compBuilder => ({
            equals: compBuilder("="),
            lte: compBuilder("<="),
            lt: compBuilder("<"),
            gt: compBuilder(">"),
            gte: compBuilder(">="),
            beginsWith: (
              val: t.TypeOf<typeof sortType>[keyof t.TypeOf<typeof sortType>] &
                string
            ) => ({
              exec: async (): Promise<
                (t.TypeOf<typeof keyType> & t.TypeOf<typeof type>)[]
              > =>
                (await doc
                  .query({
                    TableName: config.tableName,
                    KeyConditionExpression: `#x = :x and begins_with(#y, :y)`,
                    ExpressionAttributeNames: {
                      "#x": _.keys(config.hashKey)[0],
                      "#y": String(sortKey)
                    },
                    ExpressionAttributeValues: {
                      ":x": hashValue,
                      ":y": val
                    }
                  })
                  .promise()).Items as any
            }),
            between: (
              val1: t.TypeOf<typeof sortType>[keyof t.TypeOf<typeof sortType>],
              val2: t.TypeOf<typeof sortType>[keyof t.TypeOf<typeof sortType>]
            ) => ({
              exec: async (): Promise<
                (t.TypeOf<typeof keyType> & t.TypeOf<typeof type>)[]
              > =>
                (await doc
                  .query({
                    TableName: config.tableName,
                    KeyConditionExpression: `#x = :x and #y between :y and :z`,
                    ExpressionAttributeNames: {
                      "#x": _.keys(config.hashKey)[0],
                      "#y": String(sortKey)
                    },
                    ExpressionAttributeValues: {
                      ":x": hashValue,
                      ":y": val1,
                      ":z": val2
                    }
                  })
                  .promise()).Items as any
            })
          }))(
            (operand: string) => (
              val: t.TypeOf<typeof sortType>[keyof t.TypeOf<typeof sortType>]
            ) => ({
              exec: async (): Promise<
                (t.TypeOf<typeof keyType> & t.TypeOf<typeof type>)[]
              > =>
                (await doc
                  .query({
                    TableName: config.tableName,
                    KeyConditionExpression: `#x = :x and #y ${operand} :y`,
                    ExpressionAttributeNames: {
                      "#x": _.keys(config.hashKey)[0],
                      "#y": String(sortKey)
                    },
                    ExpressionAttributeValues: {
                      ":x": hashValue,
                      ":y": val
                    }
                  })
                  .promise()).Items as any
            })
          )
      }),
      delete: async (x: t.TypeOf<typeof keyType>) =>
        doc.delete({ Key: x, TableName: config.tableName }).promise()
    }))(
      t.partial(config.schema),
      t.type({ ...config.hashKey, ...config.sortKey }),
      t.type(config.hashKey),
      t.type(config.sortKey)
    )
};
