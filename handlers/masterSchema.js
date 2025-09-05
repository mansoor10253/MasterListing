import { DynamoDBClient, CreateTableCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
const stage = process.env.STAGE
const META_SYNC_LISTING_TABLE = process.env.META_SYNC_LISTING_TABLE
const client = new DynamoDBClient({ region: "eu-west-1" });

export const handler = async (event) => {
    try {
        const body = JSON.parse(event.body || "{}");

        // 1️⃣ Validation
        if (!body.tableName || typeof body.tableName !== "string") {
            return {
                statusCode: 400,
                body: JSON.stringify({ error: "tableName is required" }),
            };
        }
        if (!Array.isArray(body.attributes) || body.attributes.length === 0) {
            return {
                statusCode: 400,
                body: JSON.stringify({ error: "attributes array is required" }),
            };
        }

        body.tableName = `${body.tableName}-${stage}`;
        // 2️⃣ Extract keys
        const partitionKey = body.attributes.find(attr => attr.partionkey === true);
        if (!partitionKey) {
            return {
                statusCode: 400,
                body: JSON.stringify({ error: "Partition key is required" }),
            };
        }

        const sortKey = body.attributes.find(attr => attr.sortKey === true);

        // 3️⃣ Build AttributeDefinitions
        const attributeDefinitions = body.attributes
            .filter(attr => attr.partionkey || attr.sortKey) // only keys
            .map(attr => ({
                AttributeName: attr.column,
                AttributeType: attr.type.toUpperCase().startsWith("STRING") ? "S" :
                    attr.type.toUpperCase().startsWith("NUMBER") ? "N" : "B"
            }));

        // 4️⃣ Build KeySchema
        const keySchema = [
            { AttributeName: partitionKey.column, KeyType: "HASH" }
        ];
        if (sortKey) {
            keySchema.push({ AttributeName: sortKey.column, KeyType: "RANGE" });
        }

        // 5️⃣ Create Table
        const params = {
            TableName: body.tableName,
            AttributeDefinitions: attributeDefinitions,
            KeySchema: keySchema,
            BillingMode: "PAY_PER_REQUEST"
        };

        await client.send(new CreateTableCommand(params));

        const createdAt = new Date().toISOString();
        const allFields = JSON.stringify(body.attributes);
        const requiredFields = JSON.stringify(
            body.attributes.filter(attr => attr.required === true)
        );

        const metaParams = {
            TableName: META_SYNC_LISTING_TABLE,
            Item: {
                tableName: { S: body.tableName },   // PK
                createdAt: { S: createdAt },        // SK
                allFields: { S: allFields },
                requiredFields: { S: requiredFields },
                status: { N: "1" },
                updatedAt: { S: createdAt }
            }
        };

        await client.send(new PutItemCommand(metaParams));

        // 6️⃣ Return success
        return {
            statusCode: 200,
            headers: {
                "Access-Control-Allow-Origin": "*", // ← allow all origins
                "Access-Control-Allow-Credentials": true, // ← allow cookies if needed
            },
            body: JSON.stringify({
                message: "Table created successfully",
                tableName: body.tableName,
                defaults: {
                    status: 1,
                    createdAt: new Date().toISOString(),
                    updatedAt: new Date().toISOString()
                }
            })
        };

    } catch (error) {
        console.error("❌ Error:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: "Internal Server Error" }),
        };
    }
};
