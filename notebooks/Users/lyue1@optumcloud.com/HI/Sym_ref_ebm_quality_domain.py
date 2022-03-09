# Databricks notebook source
#import dbutils
from utils.spark import read_parquet,run_sql,registerDFasTempTable
from utils.spark import load_df_from_delimited_file,convert_dates_times_format,write_df_to_delimited
from utils.filesystem import get_files_matching_pattern_with_max_date

#read file from landing
r_file = get_files_matching_pattern_with_max_date('/mnt/cdmdataIN/landing','sym_ref_ebm_quality_domain','YYYYMMDD')
df = load_df_from_delimited_file(r_file,delimiter='|',has_header=True)

#print out table
#display(df_race)

# COMMAND ----------



#set up environment setting for writing to parquet file in Processed folder
mount_point = "/mnt/cdmdataIN"
file_name = 'sym_ref_ebm_quality_domain'
sub_folder = 'processed/reference'
full_path = 'dbfs:{}/{}/{}'.format(mount_point,sub_folder,file_name)
print(full_path)
df.write.format("parquet").mode("overwrite").save(full_path)



# COMMAND ----------

from pyspark.sql import functions as F
from utils.spark import registerDFasTempTable,run_sql,add_source_column, read_parquet,write_to_parquet

from opasnow.etl import *
import re
import os

# COMMAND ----------

mount_in = set_IN_CDM(first_time=False)

# COMMAND ----------

# Process for loading provider_hist table
def load_sym_ref_ebm_quality_domain():

    # Instantiate provider_hist. Classname(file,table,mount)
    p = Reference('sym_ref_ebm_quality_domain', 'SYM_REF_EBM_QUALITY_DOMAIN', mount_in)

    # Read the parquet file and get df
    df = p.parquet

 
    # select order of columns to be loaded as our current OPA CDM
    df = df.select( ['QUALITY_DOMAIN_ID','QUALITY_DOMAIN_NAME'])

    #bring in audit columns
    df = p.append_audit(df)
    
    # Insert the finalized dataframe to Azure synapse table
    p.append_db(df)
    #return df

# COMMAND ----------

def main():
  # Write the sequence of steps to be run in main
  load_sym_ref_ebm_quality_domain()
  

if __name__ == '__main__':
  main()