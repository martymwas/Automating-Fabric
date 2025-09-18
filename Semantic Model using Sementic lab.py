#!/usr/bin/env python
# coding: utf-8

# ## Semantic Model using Sementic lab
# 
# New notebook

# ### 1. Install or Load Semantic Link
# 
# If you’re on Spark 3.4+ in Fabric, Semantic Link is pre-installed. For Spark 3.3 or lower, or if you want to update:

# In[2]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install -U semantic-link


# ### 2. Connect and List Semantic Models
# 
# First, see which semantic models are available in your workspace.

# In[3]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install -U semantic-link
import sempy.fabric as fabric

# List all datasets (semantic models) in the workspace
df_datasets = fabric.list_datasets()
display(df_datasets)


# ### 3. Explore Tables

# In[4]:


tables_info = fabric.list_tables("SalesSample", include_columns=True).to_dict(orient="records")

# Group by table
from collections import defaultdict
tables_dict = defaultdict(list)

for entry in tables_info:
    tables_dict[entry["Name"]].append(entry["Column"])

# Print cleanly
for table, columns in tables_dict.items():
    print(f"\n📌 {table}")
    for col in columns:
        print(f"   - {col}")


# ### 4. Explore Measures

# In[5]:


import sempy.fabric as fabric

# List all measures in the SalesSample semantic model
df_measures = fabric.list_measures("SalesSample")

# Show the first 20 for preview
display(df_measures.head(20))


# ### 5. Evaluate a Simple Measure
# 
# Let’s check Total Sales by Year & Month from DimCalendar.

# In[6]:


df_sales = fabric.evaluate_measure(
    "SalesSample",
    "Total Sales",
    ["DimCalendar[Year]", "DimCalendar[Month]"]
)

display(df_sales.head(10))


# ### 6. Evaluate Multiple Measures Together
# 
# You can bring in multiple measures at once.

# In[7]:


df_metrics = fabric.evaluate_measure(
    "SalesSample",
    ["Total Sales", "Total Quantity", "Return Rate %"],
    ["DimCalendar[Year]", "DimCalendar[Month]"]
)

display(df_metrics.head(12))


# ### 7. Evaluate a Measure by Customer Region

# In[8]:


df_region_sales = fabric.evaluate_measure(
    "SalesSample",
    "Total Sales",
    ["DimCustomers[Region]"]
)

df_region_sales.sort_values("Total Sales", ascending=False).head(10)


# ### 8. Augment Your Own Data with Measures
# 
# Suppose you have a small dataset of Regions and Products — you can enrich it with semantic model measures.

# In[9]:


from sempy.fabric import FabricDataFrame

df_local = FabricDataFrame({
    "DimCustomers[Region]": ["North", "South"],
    "DimProducts[Category]": ["Electronics", "Clothing"]
})

df_augmented = df_local.add_measure(
    ["Total Sales", "Total Quantity"],
    dataset="SalesSample"
)

df_augmented


# ### 9. Run a Full DAX Query
# 
# Sometimes you want DAX flexibility

# In[10]:


df_dax = fabric.evaluate_dax(
    "SalesSample",
    """
    EVALUATE
    SUMMARIZECOLUMNS(
        'DimCalendar'[Year],
        'DimProducts'[Category],
        "Total Sales", [Total Sales],
        "Total Quantity", [Total Quantity],
        "Return Rate %", [Return Rate %]
    )
    """
)

df_dax.head(20)


# ### 

# ### 10. Data Quality Validation (Semantic Lab Functions)
# 
# Check if ProductID → ProductName is consistent in DimProducts.

# In[11]:


df_products = fabric.read_table("SalesSample", "DimProducts")

# Check functional dependency ProductID -> ProductName
violations = df_products.list_dependency_violations(
    determinant_col="ProductID",
    dependent_col="ProductName"
)

violations.head()


# ### 11. Discovering Relationships
# 
# Semantic Link can list, visualize, and validate relationships in your model.
# 
# #### Example A – List Relationships

# 

# In[12]:


import sempy.fabric as fabric

relationships = fabric.list_relationships("SalesSample")
display(relationships.head(20))


# #### Example B – Visualize Relationships

# In[13]:


from sempy.relationships import plot_relationship_metadata

plot_relationship_metadata(relationships)


# #### Example C – Validate Relationship Violations
# 
# Ensure that the keys match up correctly. Example: check if every CustomerID in FactSales has a valid entry in DimCustomers.

# In[14]:


tables = {
    "FactSales": fabric.read_table("SalesSample", "FactSales"),
    "DimCustomers": fabric.read_table("SalesSample", "DimCustomers"),
    "DimProducts": fabric.read_table("SalesSample", "DimProducts"),
    "DimStores": fabric.read_table("SalesSample", "DimStores"),
    "DimCalendar": fabric.read_table("SalesSample", "DimCalendar")
}

violations = fabric.list_relationship_violations(tables)
display(violations.head(10))

