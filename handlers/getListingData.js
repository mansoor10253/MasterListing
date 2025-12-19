import { DynamoDBClient, ScanCommand, QueryCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";

const REGION = process.env.REGION;
const STAGE = process.env.STAGE || "dev";
const META_SYNC_LISTING_TABLE = process.env.META_SYNC_LISTING_TABLE;

const ddbClient = new DynamoDBClient({ region: REGION });

export const handler = async (event) => {
    try {
        // Parse query parameters
        const { tableName, nextToken, limit } = event.queryStringParameters || {};

        if (!tableName) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: "Missing required parameter: tableName" }),
            };
        }

        const targetTableName = `${tableName}-${STAGE}`;
      
        // Validate tableName exists in metaSync
        const metaQuery = new QueryCommand({
            TableName: META_SYNC_LISTING_TABLE,
            KeyConditionExpression: "tableName = :t",
            ExpressionAttributeValues: {
                ":t": { S: targetTableName }
            },
        });

        const metaResp = await ddbClient.send(metaQuery);
     
        const metaItem = metaResp.Items?.length ? metaResp.Items[0] : null;

        if (!metaItem) {
            return {
                statusCode: 400,
                body: JSON.stringify({
                    message: `Table '${targetTableName}' not found in meta-sync listing. Please verify tableName.`,
                }),
            };
        }

        // Query the target data table
        const scanParams = {
            TableName: targetTableName,
            Limit: limit ? parseInt(limit, 10) : 10,
            ExclusiveStartKey: nextToken
                ? JSON.parse(Buffer.from(nextToken, "base64").toString("utf8"))
                : undefined,
        };

        const dataResp = await ddbClient.send(new ScanCommand(scanParams));
        const items = (dataResp.Items || []).map((item) => unmarshall(item));

        // Prepare nextToken for pagination
        const nextPageToken = dataResp.LastEvaluatedKey
            ? Buffer.from(JSON.stringify(dataResp.LastEvaluatedKey)).toString("base64")
            : null;

        // Return response
        return {
            statusCode: 200,
            headers: {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": true,
            },
            body: JSON.stringify({
                items,
                nextToken: nextPageToken,
            }),
        };
    } catch (error) {
        console.error("Error fetching listing data:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: "Internal server error",
                error: error.message || error,
            }),
        };
    }
};
