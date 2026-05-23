jdbcHostname = "192.168.1.8"
jdbcPort = 1433
jdbcDatabase = "uhip_db"

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase}"

connectionProperties = {
    "user": "AliReda",
    "password": "cr7522001",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df = spark.read.jdbc(
    url=jdbcUrl,
    table="dbo.patient",
    properties=connectionProperties
)

display(df)