import { DynamoDBClient, QueryCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { v4 as uuidv4 } from "uuid";
const META_SYNC_LISTING_TABLE = process.env.META_SYNC_LISTING_TABLE
const stage = process.env.STAGE || "dev";
const client = new DynamoDBClient({ region: "eu-west-1" });

export const handler = async (event) => {
    try {
        const body = JSON.parse(event.body || "{}");

        // 1️⃣ tableName validation
        if (!body.tableName || typeof body.tableName !== "string") {
            return {
                statusCode: 400,
                body: JSON.stringify({ error: "tableName is required" }),
            };
        }

        const targetTableName = `${body.tableName}-${stage}`;
        console.log("targetTableName********", targetTableName);
        console.log("META_SYNC_LISTING_TABLE********", META_SYNC_LISTING_TABLE);
        // 2️⃣ Fetch metadata from meta-sync-listing-<stage>
        // Since there's only one record per tableName, we use a GetItem with static createdAt
        const metaResp = await client.send(new QueryCommand({
            TableName: "meta-sync-listing-dev",
            KeyConditionExpression: "tableName = :t",
            ExpressionAttributeValues: {
                ":t": { S: targetTableName }
            }
        }));

        console.log("metaResp:", metaResp);
        const metaItem = metaResp.Items && metaResp.Items.length > 0 ? metaResp.Items[0] : null;
        if (!metaItem) {
            return {
                statusCode: 400,
                body: JSON.stringify({
                    error: "This tableName is not in DynamoDB, please give the correct table name"
                }),
            };
        }
        console.log("metaItem************",metaItem);
        
        const requiredFields = JSON.parse(metaItem.requiredFields.S || "[]");
        for (const field of requiredFields) {
            const fieldName = field.column;
            const expectedType = field.type.toLowerCase();

            if (!(fieldName in body)) {
                return {
                    statusCode: 400,
                    body: JSON.stringify({
                        error: `Field '${fieldName}' is required`
                    }),
                };
            }

            const actualValue = body[fieldName];
            if (!validateType(actualValue, expectedType)) {
                return {
                    statusCode: 400,
                    body: JSON.stringify({
                        error: `Field '${fieldName}' must be of type ${expectedType}`
                    }),
                };
            }
        }

        // 4️⃣ Insert into target table
        const nowEpoch = Date.now();
        const insertItem = {
            id: { S: body.id || uuidv4() },
            status: { N: "1" },
            createdAt: { N: nowEpoch.toString() },
            updatedAt: { N: nowEpoch.toString() }
        };

        for (const [key, value] of Object.entries(body)) {
            if (key === "tableName" || key === "metaCreatedAt") continue;
            insertItem[key] = formatDynamoValue(value);
        }

        await client.send(new PutItemCommand({
            TableName: targetTableName,
            Item: insertItem
        }));

        return {
            statusCode: 200,
            headers: {
                "Access-Control-Allow-Origin": "*", // ← allow all origins
                "Access-Control-Allow-Credentials": true, // ← allow cookies if needed
            },
            body: JSON.stringify({ message: "Record inserted successfully" })
        };

    } catch (error) {
        console.error("❌ Error:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: "Internal Server Error" }),
        };
    }
};

function validateType(value, expectedType) {
    if (expectedType === "string") return typeof value === "string";
    if (expectedType === "number") return typeof value === "number";
    if (expectedType === "date") return !isNaN(Date.parse(value));
    return true;
}

function formatDynamoValue(value) {
    if (typeof value === "string") return { S: value };
    if (typeof value === "number") return { N: value.toString() };
    if (typeof value === "boolean") return { BOOL: value };
    return { S: JSON.stringify(value) };
}
