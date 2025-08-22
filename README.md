Data Ingestion:
    1. Create the Lambda function, change permission to give AmazonS3FullAccess
    2. Upload the Lambda layer to the Lambda function
    3- Set up the snowflake database, schema and table for the file uploaded by Lambda
    4- Change the information in the config.toml file for your own snowflake account
    5- Set up the snowflake password as an environment variable in Lambda 
    6- Create an Eventbridge rule and target the Lambda function to run everyday at a given time (e.g. 2 AM EST)
    7- Set up the connection from Postgres (see postgres.txt) to Snowflake using Airbyte (Airbyte should be installed either on EC2 instance or locally)
Transformation:
    8- Install and set up dbt for transformation, initiate dbt in the project directory and install snowflake connector (Either locally or on EC2)
    9- Edit the project.yml and source.yml files as required
    10- Set up the required macros (custome schema name for example)
    11- Set up your staging models, snapshots, intermediate models and mart models
Visualisation:
    12- Run Metabase container (installed either locally or on EC2)
    13- Connect to snowflake database and create the required visualizations