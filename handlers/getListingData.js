import { DynamoDBClient, ScanCommand, QueryCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
const stage = process.env.STAGE || "dev";

const ddbClient = new DynamoDBClient({ region: "eu-west-1" });
const META_SYNC_LISTING_TABLE = process.env.META_SYNC_LISTING_TABLE

export const handler = async (event) => {
    try {
        // Read query parameters
        const { tableName, nextToken, limit } = event.queryStringParameters || {};

        if (!tableName) {
            return {
                statusCode: 400,
                body: JSON.stringify({ error: "Missing tableName parameter" }),
            };
        }

        const targetTableName = `${tableName}-${stage}`
        console.log("tableStage**************", targetTableName);

        // Validate tableName exists in metaSync
        const metaResp = await ddbClient.send(new QueryCommand({
            TableName: "meta-sync-listing-dev",
            KeyConditionExpression: "tableName = :t",
            ExpressionAttributeValues: {
                ":t": { S: targetTableName }
            }
        }));
        console.log("metaResp***********", metaResp);


        const metaItem = metaResp.Items && metaResp.Items.length > 0 ? metaResp.Items[0] : null;
        console.log("metaItem******", metaItem);
        // if (!metaItem) {
        //     return {
        //         statusCode: 400,
        //         body: JSON.stringify({
        //             error: "This tableName is not in metaSync, please give the correct table name"
        //         }),
        //     };
        // }

        // Query actual data table
        const scanParams = {
            TableName: targetTableName,
            Limit: limit ? parseInt(limit) : 10,
            ExclusiveStartKey: nextToken ? JSON.parse(Buffer.from(nextToken, "base64").toString("utf8")) : undefined
        };

        const dataResp = await ddbClient.send(new ScanCommand(scanParams));

        // Convert to plain JS objects
        const items = (dataResp.Items || []).map(item => unmarshall(item));

        return {
            statusCode: 200,
            headers: {
                "Access-Control-Allow-Origin": "*", // ← allow all origins
                "Access-Control-Allow-Credentials": true, // ← allow cookies if needed
            },
            body: JSON.stringify({
                items,
                nextToken: dataResp.LastEvaluatedKey
                    ? Buffer.from(JSON.stringify(dataResp.LastEvaluatedKey)).toString("base64")
                    : null
            }),
        };
    } catch (err) {
        console.error("❌ Error:", err);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: err.message || "Internal Server Error" }),
        };
    }
};
