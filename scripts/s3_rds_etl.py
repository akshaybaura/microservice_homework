import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

# custom transform node trf_statusLogs_explode
def FilterHighVoteCounts(glueContext, dfc) -> DynamicFrameCollection:
    """
    this function explodes(create multiple rows for objects in the iterable object) the statusLogs and returns 'updatedon'
    and 'status' as separate columns.
    :param  glueContext obj
    :param  DynamicFrame Collection (fancy name for records in a dataframe )
    :return dynamicframe
    """
    df = dfc.select(list(dfc.keys())[0]).toDF()
    if len(df.head(1)) == 0:
        dyf = DynamicFrame.fromDF(df, glueContext, "emptydf")
        return dyf
    df_filtered = df.select(
        df.userid, df.ingested_at, explode(df.statuslogs).alias("statuslogs")
    ).select(
        col("userid"),
        col("ingested_at"),
        col("statuslogs")["updatedOn"].alias("updatedOn"),
        col("statuslogs")["status"].alias("status"),
    )
    dyf_filtered = DynamicFrame.fromDF(df_filtered, glueContext, "filter_votes")
    return dyf_filtered


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node src_s3_user_status
src_s3_user_status_node1645276001944 = glueContext.create_dynamic_frame.from_catalog(
    database="user_status_db",
    table_name="table_microservice_hw",
    transformation_ctx="src_s3_user_status_node1645276001944",
)

# Simple validator SQL node for only allowing non null fields in the pipeline
SqlQuery1 = """
select *,current_timestamp as ingested_at from myDataSource
where 
userid is not null and createdAt is not null and lastVisitedAt is not null 
and status is not null and status<>'' and statusLogs is not null and isBlacklisted is not null;
"""
trf_filter_null_fields_node1645340517692 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={"myDataSource": src_s3_user_status_node1645276001944},
    transformation_ctx="trf_filter_null_fields_node1645340517692",
)

# This node is one of the validation node as above. It takes out the entire record from the pipeline if any field is null and
# puts them to the user_status_error_rec table in the RDS 
SqlQuery2 = """
select 
 'userid:'||coalesce(cast(userid as string),'null')||',createdAt:'||coalesce(cast(createdAt as string),'null')||',lastVisitedAt:'||coalesce(cast(lastVisitedAt as string),'null')||',status:'||coalesce(cast(status as string),'null')||',statusLogs:'||coalesce(cast(statusLogs as string),'null')||',isBlacklisted:'||coalesce(cast(isBlacklisted as string),'null' ) as error_rec
 from myDataSource
 where userid is null or createdAt is null or lastVisitedAt is null 
 or status is null or status='' or statusLogs is  null or isBlacklisted is null;
"""
trf_pass_error_rec_node1645353741162 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2,
    mapping={"myDataSource": src_s3_user_status_node1645276001944},
    transformation_ctx="trf_pass_error_rec_node1645353741162",
)

# This node takes out statusLogs field from the user_status table pipeline
trf_drop_statusLogs_node1645285972569 = ApplyMapping.apply(
    frame=trf_filter_null_fields_node1645340517692,
    mappings=[
        ("ingested_at", "timestamp", "ingested_at", "timestamp"),
        ("userid", "string", "userid", "string"),
        ("createdat", "string", "createdat", "string"),
        ("lastvisitedat", "int", "lastvisitedat", "int"),
        ("status", "string", "status", "string"),
        ("isblacklisted", "boolean", "isblacklisted", "boolean"),
    ],
    transformation_ctx="trf_drop_statusLogs_node1645285972569",
)

# This node removes other fields and just keeps statusLogs for the logs table pipeline
trf_pass_statusLogs_node1645286931864 = ApplyMapping.apply(
    frame=trf_filter_null_fields_node1645340517692,
    mappings=[
        ("ingested_at", "timestamp", "ingested_at", "timestamp"),
        ("userid", "string", "userid", "string"),
        ("statuslogs", "array", "statuslogs", "array"),
    ],
    transformation_ctx="trf_pass_statusLogs_node1645286931864",
)

# Script generated for node trf_statusLogs_explode
trf_statusLogs_explode_node1645303001128 = FilterHighVoteCounts(
    glueContext,
    DynamicFrameCollection(
        {
            "trf_pass_statusLogs_node1645286931864": trf_pass_statusLogs_node1645286931864
        },
        glueContext,
    ),
)

# Script generated for node trf_catch_dyfc
# This node is important as a successor to the custom transformation since SQL tranform is able to accept dynamic frame as its input
SqlQuery0 = """
select * from myDataSource

"""
trf_catch_dyfc_node1645304208094 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": trf_statusLogs_explode_node1645303001128},
    transformation_ctx="trf_catch_dyfc_node1645304208094",
)

### The following nodes push data into the 3 postgres tables- user_status, user_status_log, user_table_error_rec using JDBC connector
# Script generated for node PostgreSQL
PostgreSQL_node1645356475016 = glueContext.write_dynamic_frame.from_catalog(
    frame=trf_pass_error_rec_node1645353741162,
    database="user_status_db",
    table_name="postgres_postgres_public_user_status_error_rec",
    transformation_ctx="PostgreSQL_node1645356475016",
)

# Script generated for node PostgreSQL
PostgreSQL_node1645283219125 = glueContext.write_dynamic_frame.from_catalog(
    frame=trf_drop_statusLogs_node1645285972569,
    database="user_status_db",
    table_name="postgres_postgres_public_user_status",
    transformation_ctx="PostgreSQL_node1645283219125",
)

# Script generated for node PostgreSQL
PostgreSQL_node1645304228911 = glueContext.write_dynamic_frame.from_catalog(
    frame=trf_catch_dyfc_node1645304208094,
    database="user_status_db",
    table_name="postgres_postgres_public_user_status_log",
    transformation_ctx="PostgreSQL_node1645304228911",
)

job.commit()
