### API-server

Example local build cmd:

```bash
docker build -f Dockerfile -t nlsql-api-server .
```

Example local run cmd:

```bash
docker run --rm -p 8080:80 --env-file .env nlsql-api-server
```

Docker env params:

-   DatabaseType (_Options: 'mysql', 'mssql', 'snowflake', 'redshift', 'postgresql', 'bigquery'_)
-   DataSource (_For 'snowflake' analogue of the 'Account' db parameter_)
-   Warehouse (_Used when DataSource='snowflake'_)
-   DbSchema (_Used when DataSource='snowflake'_)
-   DbName
-   DbUser
-   DbPassword
-   DbPort (_Used when DataSource is 'mysql' or 'postgresql'_)
-   ApiEndPoint
-   ApiToken
-   AppId
-   AuthTenantID (_Optional_)
-   AppPassword
-   StaticEndPoint
-   client*email (\_Used when DataSource='bigquery'*)
-   token*uri (\_Used when DataSource='bigquery'*)
-   private*key (\_Used when DataSource='bigquery'*)
-   project*id (\_Used when DataSource='bigquery'*)

API endpoint: `/nlsql-analyzer`

Method: `POST`

JSON: `{"channel_id": str, "text": str}`

### Nginx

location `~* \.(jpg|jpeg|gif|png|css|zip|tgz|gz|rar|bz2|doc|xls|exe|pdf|ppt|tar|mid|midi|wav|bmp|rtf|js|swf|docx|xlsx|svg|csv|html)$`
to `root /var/www/html`

location `/api/messages`
to `proxy_pass http://localhost:8000`
