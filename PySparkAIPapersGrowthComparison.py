# Databricks notebook source
# DBTITLE 1,1. Initialization
# MAGIC %run "/Shared/PySparkMagClass"

# COMMAND ----------

AzureStorageAccount = '<AzureStorageAccount>'     # Azure Storage (AS) account containing MAG dataset
AzureStorageAccessKey = '<AzureStorageAccessKey>' # Access Key of the Azure Storage account
MagContainer = '<MagContainer>'                   # The container name in Azure Storage (AS) account containing MAG dataset, Usually in forms of mag-yyyy-mm-dd

# COMMAND ----------

# Create a MicrosoftAcademicGraph instance to access MAG dataset
MAG = MicrosoftAcademicGraph(container=MagContainer, account=AzureStorageAccount, key=AzureStorageAccessKey)

# COMMAND ----------

# Load MAG datas
Papers = MAG.getDataframe('Papers')
PaperFoses = MAG.getDataframe('PaperFieldsOfStudy')
Foses = MAG.getDataframe('FieldsOfStudy')
PaperAuthorAffiliations = MAG.getDataframe('PaperAuthorAffiliations')
Affiliations = MAG.getDataframe('Affiliations')

# COMMAND ----------

# DBTITLE 1,2. Find AI papers and its affiliations
#Find FieldOfStudyId for Artificial Intelligence
AiId = Foses.select(Foses.FieldOfStudyId).where(Foses.NormalizedName == "artificial intelligence")

#Join PaperFoses to get AI papers
AiPapersId = PaperFoses.join(AiId, PaperFoses.FieldOfStudyId == AiId.FieldOfStudyId, 'inner').select(PaperFoses.PaperId)

# COMMAND ----------

#Join Papers to get publish year for AI papers
AiPapers = Papers.join(AiPapersId, Papers.PaperId == AiPapersId.PaperId, 'inner').select(Papers.PaperId, Papers.Year).where((Papers.Year >= 2010) & (Papers.Year <= 2019))

# COMMAND ----------

#Join PaperAuthorAffiliations to get AffiliationId for AI papers
AiPapersWithAffiId = PaperAuthorAffiliations.join(AiPapers, PaperAuthorAffiliations.PaperId == AiPapers.PaperId, 'inner').select(AiPapers.PaperId, AiPapers.Year, PaperAuthorAffiliations.AffiliationId).where(PaperAuthorAffiliations.AffiliationId.isNotNull()).distinct()

# COMMAND ----------

#Get AffiliationIds
AiAffiliationIds = AiPapersWithAffiId.select("AffiliationId").distinct()

#Join Affiliations to get Affiliation Latitude/Longitude
AiAffiliations = Affiliations.join(AiAffiliationIds, AiAffiliationIds.AffiliationId == Affiliations.AffiliationId, 'inner').select(Affiliations.AffiliationId, Affiliations.Latitude, Affiliations.Longitude).where(Affiliations.Latitude.isNotNull()).where(Affiliations.Longitude.isNotNull())

# COMMAND ----------

# DBTITLE 1,3. Get affiliation country info using Geocoder API
import geocoder
from time import sleep

#Define function to call Geocoder API
#https://geocoder.readthedocs.io/index.html

def getlocations(lat,long):
    bing = geocoder.bing([lat, long], method='reverse')
    sleep(0.1)
    return bing.country
  
GenerateLocationsBing_udf_func = udf(getlocations)

# COMMAND ----------

#Get affiliation country info from AiAffiliations
AiAffiliationsWithCountry = AiAffiliations.select("AffiliationId", "Latitude", "Longitude", GenerateLocationsBing_udf_func("Latitude", "Longitude").alias('Country'))

# COMMAND ----------

# DBTITLE 1,4. Get year and country info for AI papers
#Join AiPapersWithAffiId to get country info for AI papers
AiPapersWithLatLongCountry = AiPapersWithAffiId.join(AiAffiliationsWithCountry, AiPapersWithAffiId.AffiliationId == AiAffiliationsWithCountry.AffiliationId, 'inner').select(AiPapersWithAffiId.PaperId, AiPapersWithAffiId.Year, AiPapersWithAffiId.AffiliationId, AiAffiliationsWithCountry.Country)

# COMMAND ----------

# DBTITLE 0,5. Generate AI papers Year-Country count
#Group by Country/Year to get AI papers count per year and country
AiPapersYearCountryGroup = AiPapersWithLatLongCountry.groupBy('Country', 'Year').count().withColumnRenamed('count', 'PaperCount')

# COMMAND ----------

# DBTITLE 1,5. Generate AI papers Year-Country Figure
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

#Convert Spark dataFrame to Pandas dataFrame for graph drawing
AiPapersYearCountryGroupPandas = AiPapersYearCountryGroup.toPandas()

# COMMAND ----------

#Group by Country to get AI papers count per country
AiPapersCountryGroupPandas = AiPapersYearCountryGroupPandas.groupby(['Country'])['PaperCount'].sum().reset_index(name ='PaperCount')

#Get list of top 5 countries
TopCountryList = AiPapersCountryGroupPandas.nlargest(5, 'PaperCount')['Country'].tolist()

#Filter top countries on AI papers count per year and country
AiPapersYearCountryGroupTopPandas = AiPapersYearCountryGroupPandas[AiPapersYearCountryGroupPandas['Country'].isin(set(TopCountryList))]

# COMMAND ----------

#Set figures size
plt.figure(figsize=(8, 6))

#Set seaborn lineplot
ax = plt.subplot()
ax = sns.lineplot(x="Year", y="PaperCount", hue="Country", style="Country", markers=['o','s','v','d', '^'], hue_order = TopCountryList, dashes=False, data=AiPapersYearCountryGroupTopPandas)

#Set figure title
ax.set_title("AI Paper Count By Year and Country (Top 5)", fontsize=18)

#Set figure legend
handles, labels = ax.get_legend_handles_labels()
ax.legend(handles=handles[1:], labels=labels[1:], loc=0, ncol=1, fontsize = 10)

#Display figure
display(plt.show())
