# Databricks notebook source
# MAGIC %md # Topic Paper Growth By Country
# MAGIC 
# MAGIC To run this notebook:
# MAGIC   - [Create an Azure Databricks service](https://azure.microsoft.com/services/databricks/).
# MAGIC   - [Create a cluster for the Azure Databricks service](https://docs.azuredatabricks.net/user-guide/clusters/create.html).
# MAGIC   - [Create a Bing Maps API Key](https://www.microsoft.com/maps/create-a-bing-maps-key).
# MAGIC   - Add Bing Maps API to Databricks Cluster Environment Variables
# MAGIC   - Install Geocoder to Cluster Libraries
# MAGIC   - [Import](https://docs.databricks.com/user-guide/notebooks/notebook-manage.html#import-a-notebook) samples/PySparkMagClass.py under Workspace **Shared** folder.
# MAGIC   - [Import](https://docs.databricks.com/user-guide/notebooks/notebook-manage.html#import-a-notebook) this notebook.
# MAGIC   - Replace **`<AzureStorageAccount>`**. This is the Azure Storage account containing MAG dataset.
# MAGIC   - Replace **`<AzureStorageAccessKey>`**. This is the Access Key of the Azure Storage account.
# MAGIC   - Replace **`<MagContainer>`**. This is the container name in Azure Storage account containing MAG dataset, usually in forms of mag-yyyy-mm-dd.
# MAGIC   - Attach this notebook to the cluster and run.

# COMMAND ----------

# DBTITLE 1,1. Initialization
# MAGIC %run "/Shared/PySparkMagClass"

# COMMAND ----------

AzureStorageAccount = '<AzureStorageAccount>'     # Azure Storage (AS) account containing MAG dataset
AzureStorageAccessKey = '<AzureStorageAccessKey>' # Access Key of the Azure Storage account
MagContainer = '<MagContainer>'                   # The container name in Azure Storage (AS) account containing MAG dataset, usually in forms of mag-yyyy-mm-dd
FosNormalizedName = 'artificial intelligence'

# COMMAND ----------

# create a MicrosoftAcademicGraph instance to access MAG dataset
MAG = MicrosoftAcademicGraph(container=MagContainer, account=AzureStorageAccount, key=AzureStorageAccessKey)

# COMMAND ----------

# load MAG data
magPapers = MAG.getDataframe('Papers')
magPaperFoses = MAG.getDataframe('PaperFieldsOfStudy')
magFoses = MAG.getDataframe('FieldsOfStudy')
magPaperAuthorAffiliations = MAG.getDataframe('PaperAuthorAffiliations')
magAffiliations = MAG.getDataframe('Affiliations')

# COMMAND ----------

# DBTITLE 1,2. Find target papers and their affiliations
# find target FieldOfStudyId
FosId = magFoses \
  .where(magFoses.NormalizedName == FosNormalizedName) \
  .select(magFoses.FieldOfStudyId)

# join magPaperFoses to get target papers
PapersId = magPaperFoses \
  .join(FosId, magPaperFoses.FieldOfStudyId == FosId.FieldOfStudyId, 'inner') \
  .select(magPaperFoses.PaperId)

# COMMAND ----------

# join magPapers to filter publish year
Papers = magPapers \
  .where((magPapers.Year >= 2010) & (magPapers.Year <= 2019)) \
  .join(PapersId, magPapers.PaperId == PapersId.PaperId, 'inner') \
  .select(magPapers.PaperId, magPapers.Year)

# COMMAND ----------

# join magPaperAuthorAffiliations to get AffiliationId
PapersWithAffiId = magPaperAuthorAffiliations \
  .join(Papers, magPaperAuthorAffiliations.PaperId == Papers.PaperId, 'inner') \
  .select(Papers.PaperId, Papers.Year, magPaperAuthorAffiliations.AffiliationId) \
  .where(magPaperAuthorAffiliations.AffiliationId.isNotNull()).distinct()

# COMMAND ----------

# get distinct AffiliationIds
AffiliationIds = PapersWithAffiId.select("AffiliationId").distinct()

# join magAffiliations to get affiliation latitude and longitude
Affiliations = magAffiliations \
  .join(AffiliationIds, AffiliationIds.AffiliationId == magAffiliations.AffiliationId, 'inner') \
  .select(magAffiliations.AffiliationId, magAffiliations.Latitude, magAffiliations.Longitude) \
  .where(magAffiliations.Latitude.isNotNull() & magAffiliations.Longitude.isNotNull())

# COMMAND ----------

# DBTITLE 1,3. Get affiliation country info using Bing Maps Geocoder API
# import libiaries
import geocoder
from time import sleep

# define function to call Bing Maps API from Geocoder
def getlocations(lat,long):
    bing = geocoder.bing([lat, long], method='reverse')
    
    #add 0.1 second time delay to wait API call return
    sleep(0.1)
    
    return bing.country
  
GenerateLocationsBing_udf_func = udf(getlocations)

# COMMAND ----------

# get affiliation country info from Affiliations
AffiliationsWithCountry = Affiliations.select("AffiliationId", "Latitude", "Longitude", GenerateLocationsBing_udf_func("Latitude", "Longitude").alias('Country'))

# COMMAND ----------

# DBTITLE 1,4. Get year and country info
# join PapersWithAffiId to get country info
PapersWithLatLongCountry = PapersWithAffiId \
  .join(AffiliationsWithCountry, PapersWithAffiId.AffiliationId == AffiliationsWithCountry.AffiliationId, 'inner') \
  .select(PapersWithAffiId.PaperId, PapersWithAffiId.Year, PapersWithAffiId.AffiliationId, AffiliationsWithCountry.Country)

# COMMAND ----------

# group by Country/Year/PaperId first to eliminate double count when authors in one paper are from different affiliations but in the same country
PapersYearCountryIdGroup = PapersWithLatLongCountry.groupBy('PaperId', 'Country', 'Year').count()

# COMMAND ----------

# DBTITLE 0,5. Generate AI papers Year-Country count
# group by Country/Year to get papers count per year and country
PapersYearCountryGroup = PapersYearCountryIdGroup.groupBy('Country', 'Year').count().withColumnRenamed('count', 'PaperCount')

# COMMAND ----------

# DBTITLE 1,5. Generate papers Year-Country Figure
# import libiaries for drawing figure
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

# convert Spark dataFrame to Pandas dataFrame for graph drawing
PapersYearCountryGroupPandas = PapersYearCountryGroup.toPandas()

# COMMAND ----------

# group by Country to get papers count per country
PapersCountryGroupPandas = PapersYearCountryGroupPandas.groupby(['Country'])['PaperCount'].sum().reset_index(name ='PaperCount')

# get list of top 5 countries
TopCountryList = PapersCountryGroupPandas.nlargest(5, 'PaperCount')['Country'].tolist()

# filter top countries on papers count per year and country
PapersYearCountryGroupTopPandas = PapersYearCountryGroupPandas[PapersYearCountryGroupPandas['Country'].isin(set(TopCountryList))]

# COMMAND ----------

# set figures size
plt.figure(figsize=(8, 6))

# set seaborn lineplot
ax = plt.subplot()
ax = sns.lineplot(x="Year", y="PaperCount", hue="Country", style="Country", markers=['o','s','v','d', '^'], hue_order = TopCountryList, dashes=False, data=PapersYearCountryGroupTopPandas)

# set figure title
ax.set_title("Paper Count By Year and Country (Top 5)", fontsize=18)

# set figure legend
handles, labels = ax.get_legend_handles_labels()
ax.legend(handles=handles[1:], labels=labels[1:], loc=0, ncol=1, fontsize = 10)

# display figure
display(plt.show())
