# Retail project using Lakeflow_Declarative_Pipelines

> [!CAUTION]
>Ce bloc apparait en rouge

> [!NOTE]
> Ce bloc apparaît en bleu.

> [!TIP]
> Ce bloc apparaît en vert.

> [!IMPORTANT]
> Ce bloc apparaît en violet.

> [!WARNING]
> Ce bloc apparaît en jaune/orange.



Ce projet est réalisé pour mettre en évidence la caapcité à développer un projet de data engineering de bout en bout avec Databricks en utilisant une approche appelée Lakeflow_Declarative_Pipeline. Pour bien montrer les fonctionnalité de Databricks on travaillera sur des données de retail.


# 🗺️ Project Phases & Guide

> [!NOTE]
> ## 🏗️ Phase1 - Project Usecase definition

<aside>

> [!TIP]
> ***Goal***: *Document à rédiger avec collaboration des métiers pour bien cadrer le besoin.Ce sera un résumé de l'existant, du problème et les solutions adaptées.Il sera lieu de dessiner une architecture technique*

## Background

- Les entreprises de retail génère une grosse volumétrie de données de CRM, d'inventaire et de transaction.
- Nous disposons des données issues de sources et de formats variés; ce qui rend compliqué l'analyse unifiée.
- Les équipes métier ont besoin de kpi(quick insight) dans les ventes(sales), les customers, products et inventory.

## Problem

- Les données sont réparties dans plusieurs sources déconnectées entre elles; ceci crée data silos.
- Les anciennes pipelines existantes sont devenues lentes avec l'augmentation des données et la mise en échelle est difficile(not scalable).
- Les métiers ont besoin d'instaurer une politique de self service BI analytic pour les opération de retail et des indicateurs de vente à travers des dashboards et Genie AI interface.

## Solution approach

- Créer une plateforme d'analyse de retail(**retail analytic plateform**) dans Databricks en utilisant une architecture de medaille(Bronze-Silver-Gold) qui est scalable et répond aux standards modernes.
- Intégrer dans le **retail analytic plateform** des données de Salesforce, PostgreSQL et Azure blob storage réglant le problème de silos
- Créer un retail Dashboard et Databricks genie AI interface permettant aux end users de faire leurs dashboards et d'interagir direcetement avec les données en utilisant un langage naturel.
  
## Design the architecture

L'image ci-dessous montre ce que nous allons techniquement faire dans ce projet:

<img width="925" height="454" alt="image" src="https://github.com/user-attachments/assets/5cbc02d9-6512-448d-8a38-61d00d5b8caa" />

---

- **Sources de données:** Nous utiliserons **3 sources de données**: Salesforce, PostgreSQL et Azure blob storage(on utilisera les free version)
- **Ingestion:** Ensuite on va ingérer les données à l'aide des fonctionnalités de Databricks **Lakeflow connect** pour Salesforce, PostgreSQL; **Auto loader** pour Azure blob storage
- **Transformations:** Pour la transformation des données dans une medaillon architecture on utilisera le composant **Lakeflow Déclarative Pipelines** de Databricks.
- **Semantic layer:** Nous mettrons en place un **Metric view** comme **semantic layer**
- **Data consum:** Pour la consommation on produira des **Databricks dashboards** et **Genie workspace**.
- **Orchestration:** Après avoir conçu la solution on utilisera des **Databricks job** pour orchestrer le tout.
- **Gouvernance: Unity Catalog de Databricks** sera utilisé comme outil de gouvernance.
- **Databricks Genie code**: va etre utilisé comme base de développement durant tout au long de ce projet dans les notebooks

**Databricks Genie code:** *Est récemment conçu par Databricks; il est très pratique en permettant de gagner en productivité et ne pas perdre trop de temps dans la conception des code manuellement*.

- **Gold layer - Modèlisation:**
Une modélisation en étoile sera conçue.dans Gold layer on aura une Fact_table et 3 Dim_tables(Dim_customer, dim_product et dim_calendar)

<img width="893" height="465" alt="image" src="https://github.com/user-attachments/assets/a656c63f-5816-496f-b33c-6e50adab6ae4" />

---
> [!NOTE]
> ## 🏗️ Phase2 - Project Initialization

<aside>

**Goal**: Préparation des étapes de développement à travers des **Backlogs**, **Users stories** et **Sprints** en utilisant **notion** ou **jira**.

</aside>

- [ ]  **Environment setup**
      
    - [ ]  **Setup PostgreSQL**
        - [ ]  Use Neon serveless postgres(https://neon.com) et utiliser un google account
        - [ ]  Donner un project name ( retail_project) et choisir une région proche de France
            - [ ]  Copier et coller quelque part le lien d'accès généré(qui sera ultérieurement utilisé)
        - [ ]  Créons les tables sources **product_catalog** et **inventory** dans Postgres en y insérant des données(avec copier coller sur sql editor du code sql disponible dans les fichiers ***01_postgres_product_history.sql*** et ***02_postgres_inventory_history.sql*** disponible dans le dossier ***00_Source_Data*** de ce repos)
    
    - [ ]  **Setup salesforce**
        - [ ]  Use salesforce trial account (30 days free trial)
        - [ ]  Conserver le username reçu par mail and reset the password
        - [ ]  Créons les tables sources **Account** & **Opportunity** en important dans salesforce Accounts et Opportunités(fichiers client & Opportunités) les fichiers ***03_salesforce_accounts_history.csv*** et ***04_salesforce_opportunities_history*** disponible dans le dossier ***00_Source_Data*** de ce repos.
                  
    - [ ]  **Setup Azure blob storage as volume**
      Azure blob storage, S3, gcp sont des stockages cloude qui peuvent stocker n'importe quel type de données(structured, semi structured, not structured).Nous pouvons créer un volume dans Databricks qui aura les memes caractéristiques de stockage que les clouds.
        - [ ]  Créer un nouveau schéma(***Volumes*** et y créer un volume ***blob_source*** dans le catalog ***retail_q***
        - [ ]  On a 2 choix de types de volume(external et managed); on choisit managed pour un volume entierement managé par Unity catalog. On aurait choisi external si on avait un adsl ou s3.
        - [ ]  Créer un répertoire dans volume nommé ***transactions_source** pour y stocker les fichiers source transactions (***05_blob_transactions_history.csv***) disponibles dans le dossier ***00_Source_Data*** de ce repos)
        - [ ]  On importe le fichier ***05_blob_transactions_history.csv*** dans le volume
        - [ ]  Copier le ***path:*** "/Volumes/nom catalog/nom schema/nom volume/nom répertoire correspondant dans notre cas à ***"/Volumes/retail_q/Volumes/blob_source/transactions_source"*** (qui sera ultérieurement utilisé)
        - [ ]  ***blob_source*** etant disponible dans un volume nous devons l'ingérer dans un nouveau schéma ***blob_bronze** du catalog ***retail_q***.

 Dans le lakeflow connect des des connecteurs sont disponibles pour salesforces, postgres et tant d'autres rendant l'option ***ingestion pipeline (lakeflow connect)*** possible mais un transfert de données d'un volume databricks à un schema (ou élémens internes à Unity catalog) n'est pas disponible car on a affaire à un transfert entre repertoires. On ne peut donc utiliser l'option ingestion pipeline (lakeflow connect) comme on l'a fait tantôt avec salesforce et postgres.Peut dans l'avenir Databricks intégrera un connecteur adapté dans le lakeflow connect.
Pour pallier à ce problème Databricks dispose d'un outil appelé ***Auto loader*** faisant référence à une ingestion interne à Databricks disponible via notebook.
        - [ ]  
        - [ ] 


- [ ]   **Data Ingestion pipeline from Postgres with Lakeflow connect(phase 1)**
    - [ ]  Launch Databricks workspace
    - [ ]  Go to Jobs & Pipelines -> Ingestion pipeline(correspondant au *Lakeflow connect* dans le repos)
    - [ ]  **Step 1/5 : Connexion**
        - [ ]  Parmi toutes les sources disponible on clique sur postgreSQL
        - [ ]  Create connexion between Postgres & Databricks(*connex_name = neon_postgres_project*), pour avoir user, passeword et le port on utilise chat gpt en collant le lien qu'on avait récupéré(*based on this url give me the username, passeword and hostname*)
    - [ ]  **Step 2/5 : Ingestion setup**
        - [ ]  Donner un nom au pipeline(***postgres_to_bronze*** et sélectionne le catalog et schema cibles(créons les respectivement ***retail_q*** et ***postgres_bronze***)
        - [ ]  Cliquez sur *create pipeline and continu*
    - [ ]  **Step 3/5 : Validating pipeline configuration**
        - [ ]  Spécifier la database, le schema et les 2 tables source à ingérer
        - [ ]  Choisir ***batch ou incrementielle ingestion:***
        - [ ]  Choisir dans ***cursor column*** la colonne ***updated_at*** pour définir la colonne ***incrémentielle*** de la table ***product_catalog***
        - [ ]  Pour la table ***product_catalog*** mettre ***history tracking*** en **ON** pour rendre la table de destination une structure **SCD2** en créant 2 colonnes supplémentaires ***start_at(même valeur que update_at par défaut) & end_at(null par défaut)***.
        - [ ]  Choisir dans ***cursor column*** la colonne ***last_stock_update_at*** pour définir la colonne ***incrémentielle*** de la table ***inventory***
        - [ ]  Pour la table ***inventory*** mettre ***history tracking*** en **OFF** pour rendre la table de destination une structure **SCD1**
              
> [!CAUTION]
> **SCD 2:** *Si un enregistrement est mise à jour ou supprimé depuis la source; alors toute la ligne sera historisée avec une valeur **end_at not null**; une nouvelle ligne avec les nouvelles valeurs vont être créées avec **end_at null** et start_at prend la valeur de la dernière end_at.2 lignes vont être updté.On créera une colonne conditionnelle nommée active basée sur la end_at is null*.
> 
> **SCD 1:** *Il n'ya pas d'historisation les enregistrements modifiées ou supprimées disparaissent directement; seules les lignes activent seront présente: c'est le cas pour la table inventory.Une ligne sera updatée par le pipeline*.

  - [ ]  **Step 4/5 : Spécifier ou stocker les données ingérées dans Databricks**
     - [ ]  Choisir le catalog ***retail_q*** et le schémas ***postgres_bronze***
  - [ ]  **Step 5/5 : Schedules(run planification) & Notifications**
    - [ ]  Add a trigger to run the job on a schedule (for example daily)
    - [ ]  Pour l'instant nous supprimons le schedule de défaut qu'on configurera plus tard en fonction des besoins d'execution
    - [ ]  Les notifications ne sont configurées qu'avec **failure** afin de ce recevoir un email en cas d'echec de pipeline execution.
    - [ ]  On clique sur **Save & Run** pour terminer la création du **ingestion pipeline** nommé ***postgres_to_bronze*** qui ingère dans le catalogue ***retail_q*** et le schema ***postgres_bronze*** de Unity catalog de databricks les tables ***product_catalog*** et ***inventory***.
    
- [ ]  **Data Ingestion pipeline from Salesforce with Lakeflow connect(phase 2)**
    - [ ]  Go to Jobs & Pipelines -> Ingestion pipeline(correspondant au *Lakeflow connect* dans le repos)
    - [ ]  **Step 1/5 : Connexion**
        - [ ]  Parmi toutes les sources disponible on clique sur Salesforce
        - [ ]  Create connexion between Salesforce & Databricks(*connex_name = Salesforce_retail_project*),
        - [ ]  On utilise les username, passeword obtenus lors de la création du salesforce account
    - [ ]  **Step 2/5 : Ingestion setup**
        - [ ]  Donner un nom au pipeline (***salesforce_to_bronze***) et on sélectionne le catalog ***retail_q*** et on crée le schéma cibles ***salesforce_bronze***
        - [ ]  Cliquez sur *create pipeline and continu*
    - [ ]  **Step 3/5 : Validating pipeline configuration**
        - [ ]  Spécifier la database, le schema et les 2 tables source à ingérer
        - [ ]  Cocher ***history tracking*** en **ON ou OFF**définit le choix entre  ***batch et incrementielle ingestion***
        - [ ]  Pas besoin de préciser un ***cursor column*** sur Salesforce car c'est déja intégré la colonne pour définir la colonne ***incrémentielle*** de la table ***account***
        - [ ]  Pour la table ***opportunity*** mettre ***history tracking*** en **OFF**.
  - [ ]  **Step 4/5 : Spécifier où stocker les données ingérées dans Databricks**
     - [ ]  Choisir le catalog ***retail_q*** et le schémas ***sales_bronze***
  - [ ]  **Step 5/5 : Schedules(run planification) & Notifications**
    - [ ]  Add a trigger to run the job on a schedule (for example daily)
    - [ ]  Pour l'instant nous supprimons le schedule de défaut qu'on configurera plus tard en fonction des besoins d'execution
    - [ ]  Les notifications ne sont configurées qu'avec **failure** afin de ce recevoir un email en cas d'echec de pipeline execution.
    - [ ]  On clique sur **Save & Run** pour terminer la création du **ingestion pipeline** nommé ***salesforce_to_bronze*** qui ingère dans le catalogue ***retail_q*** et le schema ***salesforce_bronze*** de Unity catalog de databricks les tables ***account** et ***opportunity***.

- [ ]  **Data Ingestion pipeline from Volume with Autoloader(phase 3)**
    - [ ]  **Step 1/5 : Create a folder & notebooks**
        - [ ]  Go to Databricks workspace -> Create new folder nommé ***retail_q*** dans le workspace où l'on stokera tout ce qui est important(notebooks,...)
        - [ ]  Create notebook nommé ***blob_to_bronze***
        - [ ]  Create manually a schema ***blob_bronze*** in ***retail_q*** catalog 
    - [ ]  **Step 2/5 : Autoloader code with genie**
        - [ ]  Ecrire le Autoloader code pour ingérer les fichiers
        - [ ]  Il est conseillé d'utiliser Genie code pour gagner du temps(Genie dispose 2 variants: **Agent**(run multi step data and AI tasks- ***changer le code du notebook***) & **Chat**(Asking question about your code).
        - [ ]  Choisir Agent et saisir les questions(ce qu'on souhaite faire) dans le prompt.

> [!IMPORTANT]
> **Goal of the question :** For reading csv files from source and write them into target schema using Auto loader
> 
> **Question 1:** We are getting csv files at ***"/Volumes/retail_q/Volumes/blob_source/transactions_source"***
> 
> read the csv files with Auto loader and then write them into the target table ***"retail_q.blob_bronze.transactions"***
> 
>  **Answer 1:**


      >  genie output
      
        # Databricks notebook source
        
        # Read CSV files using Auto Loader
        df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/retail_q/volumes/blob_source/_schema")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/retail_q/volumes/blob_source/transactions_source/")
       )

        # Write to bronze table - process available data and stop
         query = (df.writeStream
        .option("checkpointLocation", "/Volumes/retail_q/volumes/blob_source/_checkpoint")
        .trigger(availableNow=True)
        .toTable("retail_q.blob_bronze.transactions")
       )

        # Wait for the batch to complete
        query.awaitTermination()

        # COMMAND ----------

        # MAGIC %sql
        # MAGIC select count(*) from retail_q.blob_bronze.transactions
        

> [!CAUTION]
> **Interprétation**
> 
> **Auto Loader:** *Auto loader est adaptée pour lecture et écriture de données entre un volume et schemas se trouvant tous dans Databricks*.

**spark.readStream:** *Il n'ya pas d'historisation les enregistrements modifiées ou supprimées disparaissent directement; seules les lignes activent seront présente: c'est le cas pour la table inventory.Une ligne sera updatée par le pipeline*.

**.format("cloudFiles"):**

**.option("cloudFiles.format", "csv"):**

**.option("cloudFiles.schemaLocation", "/Volumes/retail_q/volumes/blob_source/_schema"):**

**.option("header", "true"):**

**.option("inferSchema", "true"):**
> 
**.load("/Volumes/retail_q/volumes/blob_source/transactions_source/"):**

**df.writeStream:**

**.option("checkpointLocation", "/Volumes/retail_q/volumes/blob_source/_checkpoint"):**

**.trigger(availableNow=True):**

**.toTable("retail_q.blob_bronze.transactions"):**


> [!WARNING]
> *Le code généré par genie ne doit pas être directement utilisé; il doit être réadapté car il peut contenir des imperfections:*
>
> On peut :

 *1- Faire des **commentaires** explicites pour chaque étapes*

*2- Configurer des **paths et f string function** pour éviter de taper des noms de repertoires longs passible à des erreurs*

Ci-dessous n exemple de reading csv files avec Autoloader reformatté:

<img width="983" height="573" alt="image" src="https://github.com/user-attachments/assets/07d19f59-5de2-489b-9bc7-e5fc00465407" />


   - [ ]  **Step 3/5 : Validating pipeline configuration**
        - [ ]  Spécifier la database, le schema et les 2 tables source à ingérer
        - [ ]  Cocher ***history tracking*** en **ON ou OFF**définit le choix entre  ***batch et incrementielle ingestion***
        - [ ]  Pas besoin de préciser un ***cursor column*** sur Salesforce car c'est déja intégré la colonne pour définir la colonne ***incrémentielle*** de la table ***account***
        - [ ]  Pour la table ***opportunity*** mettre ***history tracking*** en **OFF**.
  - [ ]  **Step 4/5 : Spécifier où stocker les données ingérées dans Databricks**
     - [ ]  Choisir le catalog ***retail_q*** et le schémas ***sales_bronze***
  - [ ]  **Step 5/5 : Schedules(run planification) & Notifications**
    - [ ]  Add a trigger to run the job on a schedule (for example daily)
    - [ ]  Pour l'instant nous supprimons le schedule de défaut qu'on configurera plus tard en fonction des besoins d'execution
    - [ ]  Les notifications ne sont configurées qu'avec **failure** afin de ce recevoir un email en cas d'echec de pipeline execution.
    - [ ]  On clique sur **Save & Run** pour terminer la création du **ingestion pipeline** nommé ***salesforce_to_bronze*** qui ingère dans le catalogue ***retail_q*** et le schema ***salesforce_bronze*** de Unity catalog de databricks les tables ***account** et ***opportunity***.


Dans le lakeflow connect des des connecteurs sont disponibles pour salesforces, postgres et tant d'autres rendant l'option ***ingestion pipeline (lakeflow connect)*** possible mais un transfert de données d'un volume databricks à un schema (ou élémens internes à Unity catalog) n'est pas disponible car on a affaire à un transfert entre repertoires. On ne peut donc utiliser l'option ingestion pipeline (lakeflow connect) comme on l'a fait tantôt avec salesforce et postgres.Peut dans l'avenir Databricks intégrera un connecteur adapté dans le lakeflow connect.
Pour pallier à ce problème Databricks dispose d'un outil appelé ***Autoloader*** faisant référence à une ingestion interne à Databricks disponible via notebook.
    - [ ]  Delete the file, as now creating pipeline for all tables
    - [ ]  Create new pipeline
    - [ ]  Create new SQL script in SSMS that lists all tables under SalesLT schema
**SELECT
s.name AS SchemaName,
t.name AS TableName
FROM sys.tables t
INNER JOIN sys.schemas s
ON t.schema_id = s.schema_id
WHERE s.name = 'SalesLT'**

So on pipeline create lookup activity, settings make a new source dataset and don’t select a specific table and use query option and copy the script (and uncheck first row only)
    - [ ]  Run debug and look at inputs outputs on output – see its in json
    - [ ]  Create forecah activity and connect on success
    - [ ]  On settings click items -> dynamic -> activity outputs for look for all tables -> add .values (which is the json list output)
    - [ ]  Update activities -> click pencil -> in foreach place copydata -> use SqlDBTables but select query and add dynamic content and insert:
**@{concat('SELECT * FROM ', item().SchemaName, '.', item().TableName )} // remember the space after from!!**
    - [ ]  Sink select the same parquet
We want it in format bronze/Schema/Tablename/Tablename.parquet so we make a new Parquet sink and select parameters where we can leverage the item() we used for the source. Now go back to the sink and update value to dynamic content and put in the relevant item() – make sure to use @
Now go back to parquet and under connection -> file path, update directory to @{concat( <<schema>>, ‘/’, <<table>>)}
And for file concat the tablename and .parquet
Validate and publish, go back to outer pipeline
We can either debug or trigger, so lets add trigger to trigger now
Click on link and can go monitor pipeline, each foreach is running concurrently as seen on gantt (if you need to make any changes, ensure you publish before triggering)
NEED TO UPDATE SSMS QUERY FOR mrk PRIVILEGES:
USE AdventureWorksLT2019;
GRANT SELECT ON SCHEMA::SalesLT TO mrk;
Since we made a change not in azure, we can click rerun pipeline in top left
Now you can see the files and directories in storage account
•	If you get an empty file:
“Azure blob storage does not support having empty folders. Thus, when you try to create folders (or empty folders), there will be a duplicate empty file. 
•	It is a blob storage with hierrachial namespace disabled-is that the cause? Yes, enabling hierarchical workspace will enable azure data lake which supports file and directory semantics and therefore which wouldn't create that additional file.”
•	E.g. https://stackoverflow.com/questions/76074718/additional-empty-blob-created-with-folder-names-in-azure-storage-container-not-a 

- [ ]  **Design the architecture**
    - [ ]  Read Databricks reference for the project → **LINK**
    - [ ]  Draw the data lakehouse architecture using draw.io or similar → **LINK**
- [ ]  **Create GitHub repository** → **LINK**
- [ ]  **Connect GitHub to Databricks using URL (**Workspace → Create → Git Folder)
- [ ]  **Create Lakehouse schemas (Unity Catalog) using**UI or SQL**:** `bronze` `silver` `gold`
- [ ]  **Create a volume inside bronze schema** `raw_sources`
- [ ]  Upload the 6 CSV files from engineering folder into the Bronze volume → **LINK**

<aside>

**Result:** Project is ready to start building Bronze, Silver, and Gold layers.

</aside>

---
> [!NOTE]
> ## 🥉 Phase2 - Building Bronze Layer

<aside>

**Goal**: Build the Bronze layer by ingesting all raw CSV files into Delta tables without any kind of transformations.

</aside>

- [ ]  Create a folder in the repository called `bronze` to store all scripts inside it
- [ ]  **Create Bronze notebook**
- [ ]  **Initial ingestion (manual)** For each of the 6 CSV files:
    - [ ]  Read the CSV into a DataFrame
    - [ ]  Write the DataFrame to a table in the Bronze schema using overwrite mode, and use a source-system prefix in the table name (for example `erp_` or `crm_`) to clearly identify where the data comes from.
    - [ ]  Run the script and query the bronze table to verify it is loaded correctly
- [ ]  Run the whole notebook to see if everything works successfully.
- [ ]  Commit & Push your changes to the GitHub repository

<aside>
🔥

**Bonus Advanced Task**

**Code review &** Identify repeated logic

- Create a dictionary to store file paths and table names
- Loop through the dictionary to ingest all files
</aside>

<aside>

**Result**: All 6 raw source files are ingested into dedicated Bronze tables with no transformations applied.

</aside>

---
> [!NOTE]
> ## 🥈 Phase3 - Building Silver Layer

<aside>

**Goal**: It is time to clean and transform our bronze data and load the clean results into silver layer. This is usually the most time consuming phase of the project and the fun part!

</aside>

- [ ]  Create repository structure
    - [ ]  Create a folder called `silver`
    - [ ]  Create two subfolders:  `crm` `erp`
- [ ]  For each Bronze table (6 tables)
    - [ ]  Create a Silver notebook `silver_<source>_<table_name>` e.g.  `silver_crm_cust_info`
    - [ ]  Analyze data quality using SQL and List all identified issues
        - [ ]  Find duplicates
        - [ ]  Validate string values: Check extra spaces, Identify abbreviations to normalize
        - [ ]  Validate dates values: Check Data Type, check the format, handle missing values
        - [ ]  Validate numeric values
        - [ ]  Standardize business key IDs to ensure tables can be joined correctly.
        - [ ]  Check the name of columns and table and make a plan how to rename them to something friendly.
    - [ ]  Section 1: Read data Bronze Table and Load it into a DataFrame
    - [ ]  Section 2: Transform data
        - Fix issues one by one
        - Keep transformations small and clear
        - Avoid one large transformation block
        - Use Spark SQL or PySpark (Python)
        - Before going to next transformation always check the result “df.display()”
    - [ ]  Sanity checks the final DataFrame before writing
    - [ ]  Section 3: Write the DataFrame to a new Silver Table and use a friendly name for the new table
    - [ ]  Sanity checks of silver table after writing
    - [ ]  Finalize notebook
        - [ ]  Run the full notebook end to end
        - [ ]  Review structure and readability
        - [ ]  Add comments and documentation
        - [ ]  Clone the notebook as a template for the next table
    - [ ]  Commit & Push your changes to the GitHub repository

<aside>
🔥

**Bonus Advanced Task**

- Review all 6 Silver notebooks and identify repeated code
- Reduce repetition by:
    - Using a config file with loops, or
    - Creating reusable Python functions

**Result:** Cleaner, scalable code and a strong step toward senior data engineering.

</aside>

<aside>

Result: All Bronze tables are transformed into analytics-ready Silver tables with validated data quality and standardized structure.

</aside>

---
> [!NOTE]
> ## 🥇 Phase4 - Building Gold Layer

<aside>

**Goal**: 

- Break the data model away from the source systems and introduce a new data model that is suitable for business intelligence and analytics.
- Use dimensional modeling to transform the Silver tables into a star schema with fact and dimension tables.
</aside>

- [ ]  Data Modeling Phase
    - [ ]  Understand the content of each Silver table
    - [ ]  Map each table to a business object such as customers, products, or sales
    - [ ]  Use draw.io to design the target data model. Example: `fact_sales`, `dim_customers`, `dim_products`
- [ ]  Build Gold tables - For each table in the new model:
    - [ ]  Write an SQL query
        - [ ]  Join all relevant Silver tables for the dimension or fact
        - [ ]  Ensure no duplicates after joins
        - [ ]  Validate the query output
    - [ ]  Load the result into a DataFrame
    - [ ]  Write the DataFrame to a new Gold table using a clear naming prefix such as `dim_` for dimension tables or `fact_` for fact tables.
    - [ ]  Sanity checks of gold table after writing
- [ ]  Commit & Push your changes to the GitHub repository

<aside>
🔥

**Bonus Task - Data Product Ownership**

At this point, the data is ready for analytics and your tables represents a **data product**

You are now responsible for making it reliable, clear, and easy to use

**Enhance metadata in Unity Catalog**

- Add meaningful descriptions to Gold tables
- Add clear descriptions to all important columns
- Ensure column names and meanings are easy to understand for analysts

**Define data relationships**

- Define primary keys for dimension tables
- Define foreign keys between fact and dimension tables
</aside>

<aside>

**Result**: All Silver tables are transformed into business-ready Gold tables designed for analytics and reporting.

</aside>

---
> [!NOTE]
> # Phase5 - Building the Pipeline

<aside>

Goal: Automate the end-to-end Lakehouse flow so data is processed reliably from Bronze to Silver to Gold.

</aside>

### Current Setup

- 1 Bronze notebook
- 6 Silver transformation notebooks
- 3 Gold notebooks (dimensions and facts)

To run each layer cleanly, we introduce **orchestration notebooks** that act as single entry points.

---

- [ ]  **Create Orchestration Notebooks**
    - [ ]  Silver orchestration: Create one Silver orchestration notebook that triggers all 6 Silver notebooks in sequence. Use **`dbutils.notebook.run`** to run notebookes.
    - [ ]  Silver orchestration: Create one Gold orchestration notebook that triggers all 6 Silver notebooks in sequence. Use **`dbutils.notebook.run`** to run notebookes.
- [ ]  C**reate a Databricks Job**
    - [ ]  Go to **Databricks → Jobs & Pipelines** then create Create a new Job
    - [ ]  Create a new Job and Give it a clear name, for example: `loading_bike_data_lakehouse`
    - [ ]  Add three Tasks:
        
        !image.png
        
        - [ ]  Bronze layer: bronze notebook
        - [ ]  Silver layer: silver_orchestration that triggers all other silver notebookes
        - [ ]  Gold layer: gold_orchestration that triggers all other gold notebookes
- [ ]  **Run and Validate**
    - [ ]  Click **Run All,**
    - [ ]  Monitor the job execution
    - [ ]  Ensure all tasks complete successfully
    - [ ]  Verify Bronze, Silver, and Gold tables are created correctly
- [ ]  **Schedule the Pipeline**
    - [ ]  Add a trigger to run the job on a schedule (for example daily)
    - [ ]  For the first few days: (Mointor the runes and check logs)
    - [ ]  After three days, pause or adjust the trigger as needed

---

<aside>

### 🎉 Congratulations

You’ve just built a complete **Data Lakehouse**.

This is **Lakehouse 1.0** and it represents the core foundation of real data engineering work.

</aside>

<aside>

### 🎓 Portfolio & Career Tip

You can confidently use this project as a **portfolio project**. You have my permission to do so.

If you share it on **GitHub** or **LinkedIn**, I would appreciate it if you give credit to the original source.

If you are **preparing for job interviews**, make sure you practice explaining this project:

- Why you designed the Lakehouse this way
- How data flows from Bronze to Silver to Gold
- How you ensured data quality and scalability
- How you automated everything with pipelines

Being able to clearly explain this project can be a **strong differentiator** and may be one of the reasons a company decides to hire you.

This is real, practical data engineering work.

</aside>

<aside>

### 🚀 Next Steps!

From here, you can take your Lakehouse to the next level by adding more advanced capabilities, such as:

- **Data quality checks**
    - Row counts, null checks, duplicates, and business rules
- **Reusable code and functions**
    - Shared transformation logic, configs, and utilities
- **New data sources**
    - APIs, Kafka, streaming data, and operational databases
- **CI/CD pipelines**
    - Automated testing, deployment, and environment promotion
- **Security and governance**
    - Access control, row-level security, and data masking
- **Monitoring and observability**
    - Pipeline health, alerts, and performance tracking
- **Incremental and streaming pipelines**
    - CDC, MERGE patterns, and real-time data processing

This is exactly how real-world data platforms evolve.

Strong foundations first, then continuous improvement.

</aside>


## Generate code to read a CSV file into a DataFrame 


### ###
