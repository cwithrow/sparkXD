

# converts estimated size to common units
def sizeconv(num, suffix='B'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)




def load_raw(raw_path, spark_context):

    df_noisy = spark_context.read.load(raw_path,
                 format = "com.databricks.spark.csv",                                          header="true", inferSchema = "true")
    return df_noisy

# def select_stars(raw_df):
#     # Select stars/unresolved light sources from the sample data
#     df_noisy = raw_df.filter(raw_df.type == 6)
#     df_noisy.registerTempTable("noisy")
#     return df_noisy