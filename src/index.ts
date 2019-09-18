// Copyright 2019 Volley Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//        http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import * as t from "io-ts";
import aws from "aws-sdk";
import _ from "lodash";

const region = "us-east-1";
let doc = new aws.DynamoDB.DocumentClient({ region });

const filt = <
  T1 extends {},
  T2 extends {},
  T3 extends {},
  T4 extends {},
  T5 extends {}
>(
  x: [T1, T2, T3, T4, T5]
): [T1, T2, T3, T4, T5] => _.filter(x) as any;

export default {
  config: (config: AWS.DynamoDB.ClientConfiguration) =>
    (doc = new aws.DynamoDB.DocumentClient(config)),
  define: <
    P1 extends t.Props,
    P2 extends t.Props,
    P3 extends t.Props & P1 & P2,
    P4 extends t.Props,
    P5 extends t.Props,
    P6 extends t.Props,
    P7 extends t.Props,
    P8 extends t.Props,
    P9 extends t.Props,
    P10 extends t.Props,
    P11 extends t.Props,
    P12 extends t.Props,
    P13 extends t.Props,
    P14 extends t.Props & P4 & P5,
    P15 extends t.Props & P6 & P7,
    P16 extends t.Props & P8 & P9,
    P17 extends t.Props & P10 & P11,
    P18 extends t.Props & P12 & P13,
    PSec1 extends t.LiteralC<string>,
    PSec2 extends t.LiteralC<string>,
    PSec3 extends t.LiteralC<string>,
    PSec4 extends t.LiteralC<string>,
    PSec5 extends t.LiteralC<string>
  >(config: {
    tableName: string;
    hashKey: P1;
    sortKey: P2;
    schema: P3;
    readonly secondaryIndexes: readonly [
      { name: PSec1; hashKey: P4; sortKey: P5; schema: P14 }?,
      { name: PSec2; hashKey: P6; sortKey: P7; schema: P15 }?,
      { name: PSec3; hashKey: P8; sortKey: P9; schema: P16 }?,
      { name: PSec4; hashKey: P10; sortKey: P11; schema: P17 }?,
      { name: PSec5; hashKey: P12; sortKey: P13; schema: P18 }?
    ];
  }) =>
    ((
      type,
      keyType,
      hashType,
      sortType,
      baseKeyType,
      secNameType,
      secType1,
      secType2,
      secType3,
      secType4,
      secType5
    ) => ({
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
        x: t.TypeOf<typeof hashType> & t.TypeOf<typeof sortType>
      ): Promise<
        Partial<t.TypeOf<typeof type>> &
          t.TypeOf<typeof hashType> &
          t.TypeOf<typeof sortType>
      > =>
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
      delete: async (x: t.TypeOf<typeof keyType>) =>
        doc.delete({ Key: x, TableName: config.tableName }).promise(),
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
      index: <TN extends t.TypeOf<typeof secNameType>>(
        indexName: "__zeroNilCheck" extends t.TypeOf<typeof secNameType>
          ? never
          : TN
      ) => {
        type FindType<T> = ({
          name: T;
          hashKey: any;
          sortKey: any;
          schema: any;
        }) extends t.TypeOf<typeof secType1>
          ? t.TypeOf<typeof secType1>
          : ({
              name: T;
              hashKey: any;
              sortKey: any;
              schema: any;
            }) extends t.TypeOf<typeof secType2>
          ? t.TypeOf<typeof secType2>
          : ({
              name: T;
              hashKey: any;
              sortKey: any;
              schema: any;
            }) extends t.TypeOf<typeof secType3>
          ? t.TypeOf<typeof secType3>
          : ({
              name: T;
              hashKey: any;
              sortKey: any;
              schema: any;
            }) extends t.TypeOf<typeof secType4>
          ? t.TypeOf<typeof secType4>
          : ({
              name: T;
              hashKey: any;
              sortKey: any;
              schema: any;
            }) extends t.TypeOf<typeof secType5>
          ? t.TypeOf<typeof secType5>
          : never;

        type SpecificType = any extends FindType<typeof indexName>
          ? never
          : FindType<typeof indexName>;

        type HashKeyNameType = keyof SpecificType["hashKey"];
        type HashKeyType = SpecificType["hashKey"][HashKeyNameType];

        type SortKeyNameType = keyof SpecificType["sortKey"];
        type SortKeyType = SpecificType["sortKey"][SortKeyNameType];

        type SchemaType = SpecificType["schema"];
        type QueryReturnType = Partial<SchemaType> &
          SpecificType["hashKey"] &
          SpecificType["sortKey"];

        const secondaryKey = (config.secondaryIndexes.find(
          x => x.name.value === indexName
        ) as unknown) as SpecificType;

        return {
          query: (hashValue: HashKeyType) => ({
            where: (sortKey: SortKeyNameType) =>
              (compBuilder => ({
                equals: compBuilder("="),
                lte: compBuilder("<="),
                lt: compBuilder("<"),
                gt: compBuilder(">"),
                gte: compBuilder(">="),
                between: (val1: SortKeyType, val2: SortKeyType) => ({
                  exec: async (): Promise<QueryReturnType[]> =>
                    (await doc
                      .query({
                        TableName: config.tableName,
                        IndexName: indexName,
                        KeyConditionExpression: `#x = :x and #y between :y and :z`,
                        ExpressionAttributeNames: {
                          "#x": _.keys(secondaryKey.hashKey)[0],
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
              }))((operand: string) => (val: SortKeyType) => ({
                exec: async (): Promise<(QueryReturnType)[]> =>
                  (await doc
                    .query({
                      TableName: config.tableName,
                      IndexName: indexName,
                      KeyConditionExpression: `#x = :x and #y ${operand} :y`,
                      ExpressionAttributeNames: {
                        "#x": _.keys(secondaryKey.hashKey)[0],
                        "#y": String(sortKey)
                      },
                      ExpressionAttributeValues: {
                        ":x": hashValue,
                        ":y": val
                      }
                    })
                    .promise()).Items as any
              })),
            exec: async (): Promise<QueryReturnType[]> =>
              (await doc
                .query({
                  TableName: config.tableName,
                  IndexName: indexName,
                  KeyConditionExpression: `#x = :x`,
                  ExpressionAttributeNames: {
                    "#x": _.keys(secondaryKey.hashKey)[0]
                  },
                  ExpressionAttributeValues: {
                    ":x": hashValue
                  }
                })
                .promise()).Items as any
          })
        };
      }
    }))(
      t.partial(config.schema),
      t.union([
        t.type({
          ...config.hashKey,
          ...config.sortKey
        }),
        t.type({
          ..._.get(config.secondaryIndexes[0], "hashKey"),
          ..._.get(config.secondaryIndexes[0], "sortKey")
        }),
        t.type({
          ..._.get(config.secondaryIndexes[1], "hashKey"),
          ..._.get(config.secondaryIndexes[1], "sortKey")
        }),
        t.type({
          ..._.get(config.secondaryIndexes[2], "hashKey"),
          ..._.get(config.secondaryIndexes[2], "sortKey")
        }),
        t.type({
          ..._.get(config.secondaryIndexes[3], "hashKey"),
          ..._.get(config.secondaryIndexes[3], "sortKey")
        }),
        t.type({
          ..._.get(config.secondaryIndexes[4], "hashKey"),
          ..._.get(config.secondaryIndexes[4], "sortKey")
        })
      ]),
      t.type(config.hashKey),
      t.type(config.sortKey),
      t.type({ ...config.hashKey, ...config.sortKey }),
      config.secondaryIndexes.length
        ? t.union(
            filt([
              _.get(config.secondaryIndexes[0], "name"),
              _.get(config.secondaryIndexes[1], "name"),
              _.get(config.secondaryIndexes[2], "name"),
              _.get(config.secondaryIndexes[3], "name"),
              _.get(config.secondaryIndexes[4], "name")
            ])
          )
        : undefined,
      config.secondaryIndexes[0]
        ? t.type({
            name: _.get(config.secondaryIndexes[0], "name"),
            hashKey: t.type(_.get(config.secondaryIndexes[0], "hashKey")),
            sortKey: t.type(_.get(config.secondaryIndexes[0], "sortKey")),
            schema: t.type(_.get(config.secondaryIndexes[0], "schema"))
          })
        : undefined,
      config.secondaryIndexes[1]
        ? t.type({
            name: _.get(config.secondaryIndexes[1], "name"),
            hashKey: t.type(_.get(config.secondaryIndexes[1], "hashKey")),
            sortKey: t.type(_.get(config.secondaryIndexes[1], "sortKey")),
            schema: t.type(_.get(config.secondaryIndexes[1], "schema"))
          })
        : undefined,
      config.secondaryIndexes[2]
        ? t.type({
            name: _.get(config.secondaryIndexes[2], "name"),
            hashKey: t.type(_.get(config.secondaryIndexes[2], "hashKey")),
            sortKey: t.type(_.get(config.secondaryIndexes[2], "sortKey")),
            schema: t.type(_.get(config.secondaryIndexes[2], "schema"))
          })
        : undefined,
      config.secondaryIndexes[3]
        ? t.type({
            name: _.get(config.secondaryIndexes[3], "name"),
            hashKey: t.type(_.get(config.secondaryIndexes[3], "hashKey")),
            sortKey: t.type(_.get(config.secondaryIndexes[3], "sortKey")),
            schema: t.type(_.get(config.secondaryIndexes[3], "schema"))
          })
        : undefined,
      config.secondaryIndexes[4]
        ? t.type({
            name: _.get(config.secondaryIndexes[4], "name"),
            hashKey: t.type(_.get(config.secondaryIndexes[4], "hashKey")),
            sortKey: t.type(_.get(config.secondaryIndexes[4], "sortKey")),
            schema: t.type(_.get(config.secondaryIndexes[4], "schema"))
          })
        : undefined
    )
};
