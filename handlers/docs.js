// handlers/docs.js
import spec from './openapi.json' assert { type: 'json' };

// Serve the OpenAPI JSON
export const openapi = async () => ({
  statusCode: 200,
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(spec),
});

// Serve the Swagger UI
export const docs = async () => ({
  statusCode: 200,
  headers: { 'Content-Type': 'text/html; charset=utf-8' },
  body: `<!doctype html>
<html>
  <head>
    <meta charset="utf-8"/>
    <title>MasterListing API Docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist/swagger-ui.css">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>body{margin:0} #swagger-ui{max-width:100%}</style>
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script>
    <script>
      window.onload = () => {
        SwaggerUIBundle({
          url: window.location.origin + window.location.pathname.replace(/\\/docs$/, '/openapi'),
          dom_id: '#swagger-ui'
        });
      };
    </script>
  </body>
</html>`
});
