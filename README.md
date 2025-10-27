## Data Ingestion

1. Create the Lambda function, and give it **AmazonS3FullAccess** permission.  
2. Upload the Lambda layer to the function.  
3. Set up the **Snowflake** database, schema, and table for the file uploaded by Lambda.  
4. Update the `config.toml` file with your own Snowflake account details.  
5. Set the Snowflake password as an environment variable in Lambda.  
6. Create an **EventBridge rule** targeting the Lambda function to run daily (e.g., 2 AM EST).  
7. Set up the connection from **Postgres → Snowflake** using **Airbyte** (installed on EC2 or locally).

---

## Transformation

8. Install and set up **dbt** for transformation, initialize it in the project directory, and install the Snowflake connector.  
9. Edit the `project.yml` and `source.yml` files as required.  
10. Set up the required macros (e.g., custom schema name).  
11. Create your staging models, snapshots, intermediate models, and mart models.

---

## Visualization

12. Run a **Metabase** container (either locally or on EC2).  
13. Connect to Snowflake and build your dashboards/visualizations.
